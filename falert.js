db.getCollection("alerts").aggregate([
{
    $match:{
        "patientuid" : ObjectId("6539c9558fcd7987dfe399d8"),
    }
},
    { $unwind: { path: "$alerts", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "alerts.alerttypeuid", foreignField: "_id", as: "type" } },
    { $unwind: { path: "$type", preserveNullAndEmptyArrays: true } },
{
    $match:{
        "alerts.isactive":true,
        "type.valuedescription" : "Food Alerts"
    }
},
{
    $sort:{
        "alerts.startdate":1
    }
},
{
    $project:{  
        "alertmessage":"$alerts.alertmessage"
    }
}

])