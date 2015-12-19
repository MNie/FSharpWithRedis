namespace WCFLibrary

open System
open System.ServiceModel
open System.ServiceModel.Web
open System.Collections.Generic
open System.Xml.Linq
open FSharp.Data
open FluentNHibernate.Automapping
open FluentNHibernate
open System.Runtime.Serialization
open StackExchange.Redis
open Newtonsoft.Json
open System.Diagnostics
open Utils


[<DataContract>]
[<Serializable>]
type Aircraft(badge, name, age, size, range) =
    [<DataMember>] member val Badge = badge with get,set
    [<DataMember>] member val Name = name with get,set
    [<DataMember>] member val Age = age with get,set
    [<DataMember>] member val Size = size with get,set
    [<DataMember>] member val Range = range with get,set

[<Serializable>]
[<DataContract>]
type AirportStatus(date : DateTime, listOfAircrafts) =
    [<DataMember>] member val AircraftList = listOfAircrafts with get,set
    [<DataMember>] member val Date = date with get, set

[<DataContract>]
type WrongRange(reason) =
    [<DataMember>] member val Reason = reason with get, set
        

[<ServiceContract()>]
type IAirportStatusService =
    [<OperationContract>]
    abstract FillDatabase : unit -> bool

    [<OperationContract>]
    abstract GetFromDatabase : unit -> AirportStatus array

    [<OperationContract>]
    [<FaultContract(typeof<WrongRange>)>]
    abstract GetFromDatabaseWithDate : startDate:DateTime * endDate:DateTime -> AirportStatus array

    [<OperationContract>]
    abstract ClearDatabase : unit -> bool

    [<OperationContract>]
    abstract IsEmpty : unit -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract IsNotEmpty : unit -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract Insert : singleAirportStatus:AirportStatus -> bool

    [<OperationContract>]
    [<WebGet()>]
    abstract Delete : singleAirportStatus:AirportStatus -> bool

[<ServiceBehaviorAttribute(ConcurrencyMode = ConcurrencyMode.Multiple)>]
type AirportStatusService() = 
    let collectionKey = "ApStatus"
    let connection = ConnectionMultiplexer.Connect("localhost")
    interface IAirportStatusService with
        member this.FillDatabase() =
            let db = connection.GetDatabase()
            let randomGenerator = Random()
            [1..10]
                |> Seq.map (fun y ->
                    AirportStatus(DateTime.UtcNow.AddDays(-1.*(float y)), [| for x in 1..y*2 -> Aircraft(sprintf "b%u" (y*x), randomGenerator.Next(10)*(y*x), randomGenerator.Next(10)*(x), randomGenerator.Next(10)*2*x*y, randomGenerator.Next(10)*2*x*y)|])
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
                    JsonConvert.DeserializeObject<AirportStatus> (string x)
                )
                |> Seq.toArray

        member this.GetFromDatabaseWithDate(startDate, endDate) =
            let db = connection.GetDatabase()
            if endDate < startDate then raise (FaultException<WrongRange>(WrongRange("End date is earlier than start date"), "End date is earlier than start date"))
            let resultData = 
                db.SortedSetRangeByScore(~~collectionKey, float (startDate.Ticks), float (endDate.Ticks), Exclude.None, Order.Ascending)
                |> Seq.map (fun x ->
                    JsonConvert.DeserializeObject<AirportStatus> (string x)
                )
                |> Seq.toArray
            if resultData.Length <= 0 then raise (FaultException<WrongRange>(WrongRange("Empty range"), "Empty range")) else resultData


        member this.ClearDatabase() =
            let db = connection.GetDatabase()
            db.KeyDelete(~~collectionKey)

        member this.IsEmpty() =
            let db = connection.GetDatabase()
            db.SortedSetLength(~~collectionKey) = (int64)0

        member this.IsNotEmpty() =
            let db = connection.GetDatabase()
            db.SortedSetLength(~~collectionKey) > (int64)0

        member this.Insert(singleAirportStatus) =
            let db = connection.GetDatabase()
            singleAirportStatus
            |> (fun x ->
                    (x.Date.Ticks, JsonConvert.SerializeObject x)
               )
            |> (fun (date, json) -> 
                    db.SortedSetAddAsync(~~collectionKey, ~~json, float (date)) |> ignore
                    Debug.WriteLine (sprintf "adding key %s" json)
                )
            |> ignore
            true

        member this.Delete(singleAirportStatus) =
            let db = connection.GetDatabase()
            singleAirportStatus
            |> (fun x ->
                    (x.Date.Ticks, JsonConvert.SerializeObject x)
               )
            |> (fun (date, json) -> 
                    db.SortedSetRemoveAsync(~~collectionKey, ~~json) |> ignore
                    Debug.WriteLine (sprintf "adding key %s" json)
                )
            |> ignore
            true