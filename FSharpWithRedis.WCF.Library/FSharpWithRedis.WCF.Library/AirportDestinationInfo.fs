namespace WCFLibrary

open System
open System.ServiceModel
open System.ServiceModel.Web
open System.Collections.Generic
open FluentNHibernate.Automapping
open FluentNHibernate
open System.Runtime.Serialization
open StackExchange.Redis
open Newtonsoft.Json
open System.Diagnostics
open Utils

[<Serializable>]
[<DataContract>]
type DestinationInfo(destination, destinationLatitude, destinationLongitude) =
    [<DataMember>] member val Destination = destination with get,set
    [<DataMember>] member val DestinationLatitude = destinationLatitude with get,set
    [<DataMember>] member val DestinationLongitude = destinationLongitude with get,set

[<Serializable>]
[<DataContract>]
type AirportDestinationInfo( date : DateTime, listOfDestinations) =
    [<DataMember>] member val AircraftList = listOfDestinations with get,set
    [<DataMember>] member val Date = date with get, set

[<ServiceContract()>]
type IAirportDestinationInfo = interface
    [<OperationContract>]
    abstract FillDatabase : unit -> bool

    [<OperationContract>]
    abstract GetFromDatabase : unit -> AirportDestinationInfo array

    [<OperationContract>]
    abstract ClearDatabase : unit -> bool

    [<OperationContract>]
    abstract IsEmpty : unit -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract IsNotEmpty : unit -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract Insert : singleDestinationInfo:AirportDestinationInfo -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract Delete : singleDestinationInfo:AirportDestinationInfo -> bool
end

[<ServiceBehaviorAttribute(ConcurrencyMode = ConcurrencyMode.Multiple)>]
type AirportDestinationInfoService() = 
    let collectionKey = "DestInfo"
    let connection = ConnectionMultiplexer.Connect("localhost")
    interface IAirportDestinationInfo with
        member this.FillDatabase() =
            let db = connection.GetDatabase()
            [1..10]
                |> Seq.map (fun y ->
                    AirportDestinationInfo(DateTime.UtcNow.AddDays(-1.*(float y)), [| for x in 1..10 -> DestinationInfo(sprintf "b%u" x, x, x)|])
                )
                |> Seq.map (fun x ->
                    (x.Date.Ticks, JsonConvert.SerializeObject x)
               )
                |> Seq.iter (fun (date, json) -> 
                    db.SortedSetAddAsync(~~collectionKey, ~~json, float (date)) |> ignore
                    Debug.WriteLine (sprintf "adding key %s" json)
                )
                |> ignore
            true

        member this.GetFromDatabase() =
            let db = connection.GetDatabase()
            db.SortedSetRangeByRank(~~collectionKey, 0L, -1L, Order.Ascending)
                |> Seq.map (fun x ->
                    JsonConvert.DeserializeObject<AirportDestinationInfo> (string x)
                )
                |> Seq.toArray

        member this.ClearDatabase() =
            let db = connection.GetDatabase()
            db.KeyDelete(~~collectionKey)

        member this.IsEmpty() =
            let db = connection.GetDatabase()
            db.SortedSetLength(~~collectionKey) = (int64)0

        member this.IsNotEmpty() =
            let db = connection.GetDatabase()
            db.SortedSetLength(~~collectionKey) > (int64)0

        member this.Insert(singleDestinationInfo) =
            let db = connection.GetDatabase()
            singleDestinationInfo
            |> (fun x ->
                    (x.Date.Ticks, JsonConvert.SerializeObject x)
               )
            |> (fun (date, json) -> 
                    db.SortedSetAddAsync(~~collectionKey, ~~json, float (date)) |> ignore
                    Debug.WriteLine (sprintf "adding key %s" json)
                )
            |> ignore
            true

        member this.Delete(singleDestinationInfo) =
            let db = connection.GetDatabase()
            singleDestinationInfo
            |> (fun x ->
                    (x.Date.Ticks, JsonConvert.SerializeObject x)
               )
            |> (fun (date, json) -> 
                    db.SortedSetRemoveAsync(~~collectionKey, ~~json) |> ignore
                    Debug.WriteLine (sprintf "adding key %s" json)
                )
            |> ignore
            true