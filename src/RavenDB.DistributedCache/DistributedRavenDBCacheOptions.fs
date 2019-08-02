namespace RavenDB.DistributedCache

open Raven.Client.Documents

/// Options to use to configure the RavenDB cache
[<AllowNullLiteral>]
type DistributedRavenDBCacheOptions() =
  /// The RavenDB document store to use for caching operations
  member val Store : IDocumentStore = null with get, set

  /// The RavenDB database to use (leave blank for document store default)
  member val Database = "" with get, set

  /// The RavenDB collection name to use for cache entries (defaults to "CacheEntries")
  member val Collection = "" with get, set

  /// Whether this configuration is valid
  member this.IsValid () =
    seq {
      match this.Store with null -> yield "Connection cannot be null" | _ -> ()
      }
