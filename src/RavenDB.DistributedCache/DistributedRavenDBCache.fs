namespace RavenDB.DistributedCache

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Raven.Client.Documents.Indexes
open Raven.Client.Documents.Linq
open Raven.Client.Documents.Operations.Indexes
open Raven.Client.Documents.Session
open System
open System.Collections.Generic
open System.Linq
open System.Text
open System.Threading.Tasks

/// Persistence object for a cache entry
type CacheEntry =
  { /// The Id for the cache entry
    Id                : string
    /// The payload for the cache entry (as a UTF-8 string)
    Payload           : string
    /// The ticks at which this entry expires
    ExpiresAt         : int64
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

  /// The name of the index for the ExpiredAt field
  let expIndex = sprintf "%s/ByExpiredAt" collName

  /// Create a collection Id from the given key
  let collId = sprintf "%s/%s" collName

  /// Save changes if any have occurred
  let saveChanges (sess : IAsyncDocumentSession) =
    match sess.Advanced.HasChanges with true -> (sess.SaveChangesAsync >> await') () | false -> ()

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
        dbug <| fun () -> sprintf "   Creating index %s.ExpiresAt..." opts.Collection
        PutIndexesOperation (
          IndexDefinition
            (Name = expIndex,
             Maps = HashSet<string> [ sprintf "docs.%s.Select(sess => new { sess.ExpiresAt })" collName ]))
        |> (opts.Store.Maintenance.Send >> ignore)
        dbug <| fun () -> "   ...done"
        dbug <| fun () -> "|> RavenDB cache environment check complete. Carry on..."
        environmentChecked <- true

  /// Remove entries from the cache that are expired
  let purgeExpired (sess : IAsyncDocumentSession) =
    let tix = DateTime.UtcNow.Ticks - 1L
    dbug <| fun () -> sprintf "Purging expired entries (<= %i)" tix
    sess.Query<CacheEntry>(expIndex)
      .Where(fun e -> e.ExpiresAt < tix)
      .ToList()
    |> Seq.iter (fun e -> sess.Delete e.Id)
    (sess.SaveChangesAsync >> await') ()
  
  /// Calculate ticks from now for the given number of seconds
  let ticksFromNow seconds = DateTime.UtcNow.Ticks + int64 (seconds * 10000000)

  /// Get the cache entry specified
  let getCacheEntry (key : string) (sess : IAsyncDocumentSession) =
    let entry = (collId >> sess.LoadAsync<CacheEntry> >> await >> box >> Option.ofObj) key
    match entry with Some e -> (unbox<CacheEntry> >> Some) e | None -> None

  /// Refresh (update expiration based on sliding expiration) the cache entry specified
  let refreshCacheEntry (entry : CacheEntry) (sess : IAsyncDocumentSession) =
    match entry.SlidingExpiration with
    | 0 -> ()
    | seconds -> sess.Advanced.Patch (entry.Id, (fun e -> e.ExpiresAt), ticksFromNow seconds)

  /// Get the payload for the cache entry
  let getEntry key =
    checkEnvironment ()
    use sess = newSession ()
    purgeExpired sess
    match getCacheEntry key sess with
    | Some e ->
        dbug <| fun () -> sprintf "Cache key %s found" key
        refreshCacheEntry e sess
        saveChanges sess
        UTF8Encoding.UTF8.GetBytes e.Payload
    | None ->
        dbug <| fun () -> sprintf "Cache key %s not found" key
        saveChanges sess
        null
  
  /// Update the sliding expiration for a cache entry
  let refreshEntry key =
    checkEnvironment ()
    use sess = newSession ()
    match getCacheEntry key sess with Some e ->  refreshCacheEntry e sess | None -> ()
    purgeExpired sess
    saveChanges sess
  
  /// Remove the specified cache entry
  let removeEntry (key : string) =
    checkEnvironment ()
    use sess = newSession ()
    (collId >> sess.Delete) key
    purgeExpired sess
    saveChanges sess
  
  /// Set the value of a cache entry
  let setEntry key payload (options : DistributedCacheEntryOptions) =
    checkEnvironment ()
    use sess = newSession ()
    purgeExpired sess
    let addExpiration entry = 
      match true with
      | _ when options.SlidingExpiration.HasValue ->
          { entry with ExpiresAt          = ticksFromNow options.SlidingExpiration.Value.Seconds
                       SlidingExpiration  = options.SlidingExpiration.Value.Seconds }
      | _ when options.AbsoluteExpiration.HasValue ->
          { entry with ExpiresAt = options.AbsoluteExpiration.Value.UtcTicks }
      | _ when options.AbsoluteExpirationRelativeToNow.HasValue ->
          { entry with ExpiresAt = ticksFromNow options.AbsoluteExpirationRelativeToNow.Value.Seconds }
      | _ -> entry
    let entry =
      { Id                 = collId key
        Payload            = UTF8Encoding.UTF8.GetString payload
        ExpiresAt          = Int64.MaxValue
        SlidingExpiration  = 0
        }
      |> addExpiration
    match getCacheEntry key sess with
    | Some _ ->
        sess.Advanced.Patch (entry.Id, (fun e -> e.Payload),           entry.Payload)
        sess.Advanced.Patch (entry.Id, (fun e -> e.ExpiresAt),         entry.ExpiresAt)
        sess.Advanced.Patch (entry.Id, (fun e -> e.SlidingExpiration), entry.SlidingExpiration)
    | None -> sess.StoreAsync (entry, entry.Id) |> await'
    saveChanges sess

  interface IDistributedCache with
    member __.Get key = getEntry key
    member __.GetAsync (key, _) = getEntry key |> Task.FromResult
    member __.Refresh key = refreshEntry key
    member __.RefreshAsync (key, _) = refreshEntry key |> Task.FromResult :> Task
    member __.Remove key = removeEntry  key
    member __.RemoveAsync (key, _) = removeEntry  key |> Task.FromResult :> Task
    member __.Set (key, value, options) = setEntry key value options
    member __.SetAsync (key, value, options, _) = setEntry key value options |> Task.FromResult :> Task
