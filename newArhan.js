db.getCollection("patientorders").aggregate([
{
    $match:{
        orderdate:{
        $gte: ISODate("2024-09-04T17:00:00.000Z"),
        $lte:  ISODate("2024-09-05T16:59:59.999Z")
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
        { $lookup: { from: "referencevalues", localField: "HN.religionuid", foreignField: "_id", as: "HN.religionuid" } },
    { $unwind: { path: "$HN.religionuid", preserveNullAndEmptyArrays: true } },
    {
        $addFields:{
            religionuid:"$HN.religionuid.valuedescription"
        }
    },
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
        $lookup: {
            from: 'diagnoses',
            let: {
                pid: "$patientvisituid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid', '$patientvisituid']
                        },
                    }
                },
  {
    $project: {
      diagnosistext: 1
    }
  }
            ],
            as: 'diag'
        }
    },
    
                {
        $lookup: {
            from: 'medicalhistories',
            let: {
                pid: "$patientvisituid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid', '$patientvisituid']
                        },
                    }
                },
                {
    $project:{
        pasthistorytext:1
    }
}
            ],
            as: 'pasthistory'
        }
    },
   
  
 
                {
        $lookup: {
            from: 'observations',
            let: {
                pid: "$patientvisituid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid', '$patientvisituid']
                        },
                    }
                },
                 { $unwind: { path: "$observationvalues", preserveNullAndEmptyArrays: true } },
{
    $addFields: {
        Height: {
            $cond: {
                if: { $eq: ["$observationvalues.shorttext", "Ht"] },
                then: "$observationvalues.resultvalue",
                else: null
            }
        }
    }
},
{
    $match:{
        Height:{$nin:[null,""]}
    }
},
{
    $sort:{
        observationdate:1
    }
},
{
    $project:{
        Height:"$Height"
    }
}

            ],
            as: 'pasthistory'
        }
    },
   
   
   
   
//{
//    $project:{
//        HN:"$HN.mrn",
//        fname:"$HN.firstname",
//        lname:{$ifNull:["$HN.lastname",""]},
//        "orderitemname":"$patientorderitems.orderitemname",
//    "Note":{$ifNull:["$patientorderitems.comments",""]},
//        "foodallergie":{$ifNull:["$foodalert",""]},
//        "status":"$status.valuedescription",
//        "bed":"$bed.name",
//         "ward":"$ward.name",
//           "dateofbirth":"$HN.dateofbirth",
//            "title":"$title.valuedescription",
//            foodalert: {
//        $cond: {
//          if: { $eq: ["$falert", null] },
//          then: "",
//          else: { $replaceAll: { input: "$falert", find: "\n", replacement: "" } }
//        }
//      },
//        diag:"$diag",
//        religionuid:"$religionuid",
//        pasthistory:"$pasthistory"
//    }
//},





])
