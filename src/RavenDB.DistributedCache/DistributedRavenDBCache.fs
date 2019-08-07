namespace RavenDB.DistributedCache

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Raven.Client.Documents
open Raven.Client.Documents.Indexes
open Raven.Client.Documents.Linq
open Raven.Client.Documents.Operations.Indexes
open Raven.Client.Documents.Session
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// Persistence object for a cache entry
[<CLIMutable>]
type CacheEntry =
  { /// The Id for the cache entry
    Id                : string
    /// The payload for the cache entry (as a UTF-8 string)
    Payload           : string
    /// The ticks at which this entry expires
    ExpiresAt         : DateTime
    /// The number of seconds in the sliding expiration
    SlidingExpiration : int
    }

/// IDistributedCache implementation utilizing RavenDB
[<AllowNullLiteral>]
type DistributedRavenDBCache(options : IOptions<DistributedRavenDBCacheOptions>,
                             log     : ILogger<DistributedRavenDBCache>) =
  
  /// Whether the environment has been checked to ensure that the database, table, and relevant indexes exist
  static let mutable environmentChecked = false
  
  do
    match options with
    | null | _ when isNull options.Value -> nullArg "options"
    | _ when isNull options.Value.Store -> nullArg "Store"
    | _ -> ()

  /// Options
  let opts = options.Value

  let await  task = task |> (Async.AwaitTask >> Async.RunSynchronously)
  let await' task = task |> (Async.AwaitIAsyncResult >> Async.Ignore >> Async.RunSynchronously)

  /// Create a new RavenDB session
  let newSession () =
    match isNull opts.Database || opts.Database = "" with
    | true -> opts.Store.OpenAsyncSession ()
    | false -> opts.Store.OpenAsyncSession opts.Database

  /// The name of the document collection used to store entries
  let collName =
    match isNull opts.Collection || opts.Collection = "" with true -> "CacheEntries" | false -> opts.Collection

  /// The name of the index for the ExpiresAt field
  let expIndex = sprintf "%s/ByExpiresAt" collName

  /// Create a document Id from the given key
  let docId = sprintf "%s/%s" collName

  /// Save changes if any have occurred
  let saveChanges (sess : IAsyncDocumentSession) ct = sess.SaveChangesAsync ct |> await'

  /// Debug message
  let dbug text =
    match log.IsEnabled LogLevel.Debug with
    | true -> text () |> sprintf "[%s] %s" opts.Collection |> log.LogDebug
    | _ -> ()

  /// Make sure the expiration index exists
  let checkEnvironment () =
    match environmentChecked with
    | true -> dbug <| fun () -> "Skipping environment check because it has already been performed"
    | _ ->
        dbug <| fun () -> "|> Ensuring proper RavenDB cache environment"
        // Index
        dbug <| fun () -> sprintf "   Creating index %s.ExpiresAt..." collName
        PutIndexesOperation (
          IndexDefinition
            (Name = expIndex,
             Maps = HashSet<string> [ sprintf "docs.%s.Select(sess => new { sess.ExpiresAt })" collName ]))
        |> (opts.Store.Maintenance.Send >> ignore)
        dbug <| fun () -> "   ...done"
        dbug <| fun () -> "|> RavenDB cache environment check complete. Carry on..."
        environmentChecked <- true

  /// Remove entries from the cache that are expired
  let purgeExpired (sess : IAsyncDocumentSession) ct =
    let now = DateTime.Now
    dbug <| fun () -> sprintf "Purging expired entries (<= %s)" <| now.ToString "o"
    let expired =
      sess.Query<CacheEntry>(expIndex).Where(fun e -> e.ExpiresAt < now).ToListAsync ct
      |> await
      |> Seq.map (fun e -> e.Id)
      |> List.ofSeq
    match List.length expired with
    | 0 -> ()
    | _ ->
        expired
        |> List.iter (fun docId ->
            dbug <| fun () -> sprintf "Deleting expired session %s" docId
            sess.Delete docId)
        saveChanges sess ct
  
  /// Get the cache entry specified
  let getCacheEntry (key : string) (sess : IAsyncDocumentSession) ct =
    sess.LoadAsync<CacheEntry> (docId key, ct) |> (await >> box >> Option.ofObj)
    |> function Some e -> (unbox<CacheEntry> >> Some) e | None -> None

  /// Refresh (update expiration based on sliding expiration) the cache entry specified
  let refreshCacheEntry (entry : CacheEntry) (sess : IAsyncDocumentSession) ct =
    match entry.SlidingExpiration with
    | 0 -> ()
    | seconds ->
        sess.Advanced.Patch (entry.Id, (fun e -> e.ExpiresAt), (float >> DateTime.Now.AddSeconds) seconds)
        saveChanges sess ct

  /// Get the payload for the cache entry
  let getEntry key ct =
    checkEnvironment ()
    use sess = newSession ()
    purgeExpired sess ct
    match getCacheEntry key sess ct with
    | Some e ->
        dbug <| fun () -> sprintf "Cache key %s found" key
        refreshCacheEntry e sess ct
        Convert.FromBase64String e.Payload
    | None ->
        dbug <| fun () -> sprintf "Cache key %s not found" key
        null
  
  /// Update the sliding expiration for a cache entry
  let refreshEntry key ct =
    checkEnvironment ()
    use sess = newSession ()
    match getCacheEntry key sess ct with Some e ->  refreshCacheEntry e sess ct | None -> ()
    purgeExpired sess ct
  
  /// Remove the specified cache entry
  let removeEntry (key : string) ct =
    checkEnvironment ()
    use sess = newSession ()
    purgeExpired sess ct
    dbug <| fun () -> sprintf "Removing key %s" key
    (docId >> sess.Delete) key
    saveChanges sess ct
  
  /// Set the value of a cache entry
  let setEntry key payload (options : DistributedCacheEntryOptions) ct =
    checkEnvironment ()
    use sess = newSession ()
    purgeExpired sess ct
    let addExpiration entry = 
      match true with
      | _ when options.AbsoluteExpiration.HasValue ->
          { entry with ExpiresAt = options.AbsoluteExpiration.Value.DateTime }
      | _ when options.AbsoluteExpirationRelativeToNow.HasValue ->
          { entry with ExpiresAt = DateTime.Now + options.AbsoluteExpirationRelativeToNow.Value }
      | _ when options.SlidingExpiration.HasValue ->
          { entry with ExpiresAt         = DateTime.Now + options.SlidingExpiration.Value
                       SlidingExpiration = options.SlidingExpiration.Value.Seconds }
      | _ -> entry
    let entry =
      { Id                 = docId key
        Payload            = Convert.ToBase64String payload
        ExpiresAt          = DateTime.MaxValue
        SlidingExpiration  = 0
        }
      |> addExpiration
    match getCacheEntry key sess ct with
    | Some _ ->
        sess.Advanced.Patch (entry.Id, (fun e -> e.Payload),           entry.Payload)
        sess.Advanced.Patch (entry.Id, (fun e -> e.ExpiresAt),         entry.ExpiresAt)
        sess.Advanced.Patch (entry.Id, (fun e -> e.SlidingExpiration), entry.SlidingExpiration)
    | None -> sess.StoreAsync (entry, entry.Id, ct) |> await'
    saveChanges sess ct

  interface IDistributedCache with
    member __.Get key = getEntry key CancellationToken.None
    member __.GetAsync (key, ct) = getEntry key ct |> Task.FromResult
    member __.Refresh key = refreshEntry key CancellationToken.None
    member __.RefreshAsync (key, ct) = refreshEntry key ct |> Task.FromResult :> Task
    member __.Remove key = removeEntry  key CancellationToken.None
    member __.RemoveAsync (key, ct) = removeEntry key ct |> Task.FromResult :> Task
    member __.Set (key, value, options) = setEntry key value options CancellationToken.None
    member __.SetAsync (key, value, options, ct) = setEntry key value options ct |> Task.FromResult :> Task
