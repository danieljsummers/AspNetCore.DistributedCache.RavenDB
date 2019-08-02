/// Extensions for <see cref="IServiceCollection" /> to add the RavenDB cache
[<AutoOpen>]
[<System.Runtime.CompilerServices.Extension>]
module RavenDB.DistributedCache.IServiceCollectionExtensions

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.DependencyInjection
open System

type IServiceCollection with

  member this.AddDistributedRavenDBCache (options : Action<DistributedRavenDBCacheOptions>) =
    match options with null -> nullArg "options" | _ -> ()
    this.AddOptions()
      .Configure(options)
      .Add (ServiceDescriptor.Transient<IDistributedCache, DistributedRavenDBCache>())
    this

/// <summary>
/// Add RavenDB options to the services collection
/// </summary>
/// <param name="options">An action to set the options for the cache</param>
/// <returns>The given <see cref="IServiceCollection" /> for further manipulation</returns>
[<System.Runtime.CompilerServices.Extension>]
let AddDistributedRavenDBCache (this : IServiceCollection, options : Action<DistributedRavenDBCacheOptions>) =
  this.AddDistributedRavenDBCache options