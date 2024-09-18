db.getCollection("patientorders").aggregate([
{
    $match:{
        orderdate:{
            $gte:ISODate("2024-07-13T00:00:00.000+07:00"),
            $lte:ISODate("2024-07-13T23:59:59.999+07:00"),
        }
    }
},
    { $unwind: { path: '$patientorderitems', preserveNullAndEmptyArrays: true } },
{
    $match:{
        "patientorderitems.ordercattype":"DIET"
    }
},
   { $lookup: { from: "frequencies", localField: "patientorderitems.frequencyuid" , foreignField: "_id", as: "fre" } },
    { $unwind: { path: "$fre", preserveNullAndEmptyArrays: true } },
   { $lookup: { from: "patients", localField: "patientuid" , foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
   { $lookup: { from: "patients", localField: "patientuid" , foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
       { $lookup: { from: "patientvisits", localField: "patientvisituid" , foreignField: "_id", as: "visit" } },
    { $unwind: { path: "$visit", preserveNullAndEmptyArrays: true } },
{
    $match:{
  "fre.code":"BREAKFAST",
  "patientorderitems.orderitemname" :  {$regex:/อาหารสายยาง/i} 
    }
},
        {
        $lookup: {
            from: 'allergies',
            let: {
                pid: "$patientuid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid', '$patientuid']
                        },
                    }
                },
 { $unwind: { path: "$foodallergies", preserveNullAndEmptyArrays: true } },      
    { $lookup: { from: "orderresultitems", localField: "foodallergies.resultitemuid", foreignField: "_id", as: "foodallergie" } },
    { $unwind: { path: "$foodallergie", preserveNullAndEmptyArrays: true } },
{
    $addFields:{
        foodalg:{$concat:["$foodallergie.name"," : ",{$ifNull:["$foodallergies.comments",""]}]}
    }
},
{
    $project:{
        foodalert:"$foodalg"
    }
}
            ],
            as: 'foodalert'
        }
    },
 {
    $addFields: {
      foodalert: {
        $reduce: {
          input: "$foodalert",
          initialValue: "",
          in: {
            $concat: [
              "$$value",
              { $cond: [{ $eq: ["$$value", ""] }, "", ", " ] },
              "$$this.foodalert"
            ]
          }
        }
      }
    }
  },
        {
        $lookup: {
            from: 'alerts',
            let: {
                pid2: "$patientuid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid2', '$patientuid']
                        },
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
        "alertsMge":"$alerts.alertmessage"
    }
}
            ],
            as: 'falert'
        }
    },
  {
    $addFields: {
      falert: {
        $reduce: {
          input: "$falert",
          initialValue: "",
          in: {
            $concat: [
              "$$value",
              { $cond: [{ $eq: ["$$value", ""] }, "", ", " ] },
              "$$this.alertsMge"
            ]
          }
        }
      }
    }
  },
    { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    {
        $match:{
          "status.valuecode" : "ORDSTS3"
        }
    }
,
    { $lookup: { from: "beds", localField: "beduid", foreignField: "_id", as: "bed" } },
    { $unwind: { path: "$bed", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "wards", localField: "warduid", foreignField: "_id", as: "ward" } },
    { $unwind: { path: "$ward", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "referencevalues", localField: "HN.titleuid", foreignField: "_id", as: "title" } },
    { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },

{
    $project:{
        HN:"$HN.mrn",
        fname:"$HN.firstname",
        lname:{$ifNull:["$HN.lastname",""]},
        "orderitemname":"$patientorderitems.orderitemname",
    "Note":{$ifNull:["$patientorderitems.comments",""]},
       
        "foodallergie":{$ifNull:["$foodalert",""]},
        "status":"$status.valuedescription",
        "bed":"$bed.name",
         "ward":"$ward.name",
           "dateofbirth":"$HN.dateofbirth",
            "title":"$title.valuedescription",
            
            foodalert: {
        $cond: {
          if: { $eq: ["$falert", null] },
          then: "",
          else: { $replaceAll: { input: "$falert", find: "\n", replacement: "" } }
        }
      }
    }
}



])
