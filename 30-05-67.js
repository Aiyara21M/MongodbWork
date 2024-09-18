db.getCollection("patientvisits").aggregate([
{
    $match:{
        _id:ObjectId("6656cc1fe5d877000193636f")
    }
},
        { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
        { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "referencevalues",localField: "entypeuid",foreignField: "_id",as: "type"}},
        { $unwind: { path: "$type",preserveNullAndEmptyArrays: true}},
   
         { $unwind: { path: "$visitpayors", preserveNullAndEmptyArrays: true } },
             {
    $addFields: {
        payor1: {
            $cond: {
                if: { $eq: ["$visitpayors.orderofpreference", 1] },
                then: "$visitpayors.payoruid",
                else: null
            }
        }
    }
 },
              {
    $addFields: {
        payor2: {
            $cond: {
                if: { $eq: ["$visitpayors.orderofpreference", 2] },
                then: "$visitpayors.payoruid",
                else: null
            }
        }
    }
 },
               {
    $addFields: {
        payor3: {
            $cond: {
                if: { $eq: ["$visitpayors.orderofpreference", 3] },
                then: "$visitpayors.payoruid",
                else: null
            }
        }
    }
 },
    { $lookup: { from: "payors", localField: "payor1", foreignField: "_id", as: "payor1" } },
    { $unwind: { path: "$payor1", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "payors", localField: "payor2", foreignField: "_id", as: "payor2" } },
    { $unwind: { path: "$payor2", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "payors", localField: "payor3", foreignField: "_id", as: "payor3" } },
    { $unwind: { path: "$payor3", preserveNullAndEmptyArrays: true } },
   { $unwind: { path: "$bedoccupancy",preserveNullAndEmptyArrays: true}},
        {
            $group:{
                _id:"$_id",
                AN:{"$first":"$visitid"},
                fname:{"$first":"$HN.firstname"},
                lname:{"$first":"$HN.lastname"},
                payor1:{"$max":"$payor1.name"},
                payor2:{"$max":"$payor2.name"},
                payor3:{"$max":"$payor3.name"},
                HN:{"$first":"$HN.mrn"},
                bedoccupancy:{"$first":"$bedoccupancy.startdate"},
                startdate:{
                "$first":{
                $cond: {
                    if: { $eq: ["$type.relatedvalue", "IPD"] },
                    then: "$startdate",
                    else: ""
                }
            }
        }
            }
        },
        {
            $project:{
                _id:1,
                AN:1,
                Fullname:{ $concat: ["$fname", " ", "$lname"] },
                payor1:1,
                payor2:1,
                payor3:1,
                HN:1,
                startdate:1,
        bedoccupancy: 1
           }
        },
            
                
])

