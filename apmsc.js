db.getCollection("appointmentschedules").aggregate([

      { $unwind: { path: "$slots" } },
    { $lookup: { from: 'patients', localField: 'slots.patientuid', foreignField: '_id', as: 'patient' } },
    { $unwind: { path: '$patient', preserveNullAndEmptyArrays: true } },
      { $lookup: { from: "referencevalues", foreignField: "_id", localField: "slots.statusuid", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    {
        $match:{
            "slots.patientuid" : ObjectId("6539d00d8fcd796494fbd0c1"),
            "iscancelled" : false,
          "status.valuecode" : "BOKSTS6"
        }
    },            
//    {
//        $sort:{
//            "slots.start":1
//        }
//    },
//{
//    $group:{
//        _id:1,
//        time:{"$last":"$slots.start"}
//    }
//}
    

    
])