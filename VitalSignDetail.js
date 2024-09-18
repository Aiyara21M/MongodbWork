db.getCollection("patientvisits").aggregate([
{
    $match:{
			_id:ObjectId("665c1eebc2aded00015ef209")
    }
},
        { $lookup: { from: "observations", localField: "_id", foreignField: "patientvisituid", as: "detail" } },
        { $unwind: { path: "$detail", preserveNullAndEmptyArrays: true } },
        { $unwind: { path: "$visitcareproviders", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "departments", localField: "visitcareproviders.departmentuid", foreignField: "_id", as: "depart" } },
        { $unwind: { path: "$depart", preserveNullAndEmptyArrays: true } },
        { $unwind: { path: "$visitjourneys", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "departments", localField: "visitjourneys.departmentuid", foreignField: "_id", as: "depart2" } },
        { $unwind: { path: "$depart2", preserveNullAndEmptyArrays: true } },
        {
            $group:{
                _id:"$_id",
                startdate:{"$first":"$startdate"},
               departdocdoc:{"$first":"$depart.name"},
               departjn:{"$first":"$depart2.name"},
                patientuid:{"$first":"$patientuid"},
                comment :{"$last":"$visitjourneys.comments"},
                visitid:{"$first":"$visitid"},
                detail:{"$first":"$detail.observationvalues"},
                bedoccupancy:{"$first":"$bedoccupancy"}
            }
        },
{
    $match:{
        comment:{"$nin":["Close Visit"]}
    }
},
{ $unwind: { path: "$detail", preserveNullAndEmptyArrays: true } },
{
    $addFields: {
        Height: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "Ht"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
},
    {
    $addFields: {
        weight: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "Wt"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
     {
    $addFields: {
        Temperature: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "T"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
      {
    $addFields: {
        Pulse: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "P"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
       {
    $addFields: {
        BMI: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "BMI"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
        {
    $addFields: {
        Sys: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "Sys"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
         {
    $addFields: {
        Dia: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "Dia"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
          {
    $addFields: {
        R: {
            $cond: {
                if: { $eq: ["$detail.shorttext", "R"] },
                then: "$detail.resultvalue",
                else: null
            }
        }
    }
 },
           {
            $group:{
                _id:"$_id",
               departdocdoc:{"$first":"$departdocdoc"},
               departjn:{"$first":"$departjn"},
               patientuid:{"$first":"$patientuid"},
               visitid:{"$first":"$visitid"},
               startdate:{"$first":"$startdate"},
               Height:{"$max":"$Height"},
               weight:{"$max":"$weight"},
               Temperature:{"$max":"$Temperature"},
               Pulse:{"$max":"$Pulse"},
               Sys:{"$max":"$Sys"},
               Dia:{"$max":"$Dia"},
               R:{"$max":"$R"},
               BMI:{"$max":"$BMI"},
               bedoccupancy:{"$first":"$bedoccupancy"}
            }
        }, 
       { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
        { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
        { $unwind: { path: "$bedoccupancy", preserveNullAndEmptyArrays: true } },

//                   {
//            $project:{
//                _id:"$_id",
//                department:"$departdocdoc",
//                departjn:1,
//                visitid:1,
//                HN:"$HN.mrn",
//                startdate:1,
//               Height:1,
//               weight:1,
//               Temperature:1,
//               Pulse:1,
//               Sys:1,
//               Dia:1,
//               BMI:1,
//               R:1
//            }
//        }, 
//        {
//            $sort:{
//                visitid:1
//            }
//        }
])
