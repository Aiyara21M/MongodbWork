db.getCollection("patientorders").aggregate([
    {
        $match: {
            orderdate: {
                $gte: ISODate("2024-05-02T00:00:00.000+07:00"),
                $lte: ISODate("2024-05-02T23:59:59.999+07:00")
            },
        }
    },
    { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "orderitems", localField: "patientorderitems.orderitemuid", foreignField: "_id", as: "item" } },
    { $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: 'wards', localField: 'warduid', foreignField: '_id', as: 'ward' } },
        { $unwind: { path: '$ward', preserveNullAndEmptyArrays: true } },
    {
        $match: {
            "item.code": {
                $in: ["PIKCLFG010ML","PIADREG001ML","PIBICAG375GM","PIBRONG025MG","PICALCG010PC","PICARDO010MG", "PIDOBUG250MG",
                    "PIDOPAG250MG","PIENOXG040MG", "PIENOXG060MG","PIFRAXO004ML", "PIFRAXO006ML","PIHUMRO010ML","PIINS3G010ML",
                    "PIINSNG010ML","PILANOO500MG","PILEVOO004ML","PIMAGNG002ML","PIMORPG010MG","PINALOG004MG","PINITRG010MG","PINURAG005ML",
                    "PIOXYTG010IU", "PIPETHG050MG", "PIPOTAG010ML", "PSCHLOG001ML","PTBEFAG003MG", "PTBEFAG005MG","PTLANOO062MG","PTLANOO250MG"]
            }
        }
    },
        { $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "VN" } },
        { $unwind: { path: "$VN", preserveNullAndEmptyArrays: true } },
       { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
       { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
       { $lookup: { from: "referencevalues", localField: "VN.entypeuid", foreignField: "_id", as: "type" } },
       { $unwind: { path: "$type", preserveNullAndEmptyArrays: true } },
       { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
       { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },

    {
        $match:{
            "type.valuecode":"ENTYPE2",
            "VN.bedoccupancy": { $ne: null },
            "status.valuecode": { $nin: ["ORDSTS5","ORDSTS4"] },
        }
    },
    {
        $group:{
            _id:{
                _id:"$_id",
                HN:"$HN.mrn",
                Fname:"$HN.firstname",
                HAD:"$item.name",
               orderdate:"$orderdate",
            },
            badmove:{"$first":"$VN.bedoccupancy"},
            carepovider:{"$last":"$VN.visitcareproviders"}
        }
    },
//            { $unwind: { path: "$badmove", preserveNullAndEmptyArrays: true } },
//{
//    $match:{
//                   $expr: {
//                $gte: ["$_id.orderdate", "$badmove.startdate"],
//            }
//    }
//},
//{
//    $group:{
//        _id:"$_id",
//        badmove:{"$last":"$badmove.warduid"}
//    },
//},
//        { $lookup: { from: 'wards', localField: 'badmove', foreignField: '_id', as: 'ward' } },
//        { $unwind: { path: '$ward', preserveNullAndEmptyArrays: true } },






//{
//    $sort:{
//        HN:1
//    }
//}


])