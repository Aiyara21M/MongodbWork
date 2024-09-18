db.getCollection("patientbills").aggregate([
 {
     $match: {
       "patientvisituid":ObjectId("660a74ccf6b0100001d94318"),
        billdate:{
         $gte : ISODate("2024-04-01T00:00:00.000+07:00"),
        $lte : ISODate("2024-04-01T23:59:59.999+07:00") 
         },
     }
       },
{ $unwind: { path: "$patientbilleditems",preserveNullAndEmptyArrays: true}},
     {
        $lookup: {
            from: 'patientorders',
            let: {
                orderitemuid: '$patientbilleditems.orderitemuid',
                patientorderuid: '$patientbilleditems.patientorderuid'
            },
            pipeline: [
                {
                    $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true }
                },
                {
                    $match: { $expr: {$and: [
                                { $eq: ['$$patientorderuid', '$_id'] },
                                { $eq: ['$$orderitemuid', '$patientorderitems.orderitemuid'] }
                                            ]  }  }
                },
                                {
                    $unwind: { path: "$patientorderitems.comments", preserveNullAndEmptyArrays: true }
                        },
                {$project: {
                        _id:0,
                        comments: "$patientorderitems.comments",
                    }},

//                 {$group: {
//                        _id:"$_id",
//                        comments: {"$last":"$comments.comments"},
//                    }}
            ],
            as: 'comments'
        }
    },
])