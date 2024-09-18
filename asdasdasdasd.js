db.getCollection("patientorders").aggregate([
    {
        $match: {
            orderdate: {
                $gte: ISODate("2024-05-01T00:00:00.000+07:00"),
                $lte: ISODate("2024-05-01T23:59:59.999+07:00")
            },
        }
    },
    { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: 'departments', localField: 'ordertodepartmentuid', foreignField: '_id', as: 'depart' } },
        { $unwind: { path: '$depart', preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "orderitems", localField: "patientorderitems.orderitemuid", foreignField: "_id", as: "item" } },
    { $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
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
        { $lookup: { from: 'wards', localField: 'warduid', foreignField: '_id', as: 'ward' } },
        { $unwind: { path: '$ward', preserveNullAndEmptyArrays: true } },
    {
        $match:{
            "type.valuecode":"ENTYPE2",
//            "VN.bedoccupancy": { $ne: null },
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
                ordernumber:"$ordernumber",
               ward01:"$ward.code"
            },   
            caredepart:{"$last":"$VN.visitcareproviders"},
            badmove:{"$first":"$VN.bedoccupancy"}
        }
    },  
{ $unwind: { path: "$badmove", preserveNullAndEmptyArrays: true } },    
        { $lookup: { from: 'wards', localField: 'warduid', foreignField: '_id', as: 'ward' } },
        { $unwind: { path: '$ward', preserveNullAndEmptyArrays: true } },
    {
    $group: {
        _id: "$_id",
          caredepart: {"$first":"$caredepart"},
        badmove: {
            $last: {
                $cond: {
                    if: {
                        $gte: ["$_id.orderdate", "$badmove.startdate"]
                    },
                    then: "$badmove.warduid",
                    else: "$$REMOVE"
                }
            }
        }
    }
}  ,
        { $unwind: { path: '$caredepart', preserveNullAndEmptyArrays: true } },
    {
    $group: {
        _id: "$_id",
        caredepart:{"$last":"$caredepart.departmentuid"} ,
        badmove:{"$last":"$badmove"}
    }
}  ,
       { $lookup: { from: 'wards', localField: 'badmove', foreignField: '_id', as: 'badmoveward' } },
        { $unwind: { path: '$badmoveward', preserveNullAndEmptyArrays: true } },
        { $lookup: { from: 'departments', localField: 'caredepart', foreignField: '_id', as: 'caredepartward' } },
        { $unwind: { path: '$caredepartward', preserveNullAndEmptyArrays: true } },
        {
            $match:{
                "_id.ward01": { $nin: [/^ICU/i] },
                "caredepartward.code":{$in:[/^IP/i,/^IM/i,/^LR/i]}
            }
        },
{
    $project:{
         _id:"$_id._id",
        HN:"$_id.HN",
        Fname:"$_id.Fname",
        HAD:"$_id.HAD",
        orderdate:"$_id.orderdate",
        ordernumber:"$_id.ordernumber",
        middepart:"$_id.ward01",
        lastdepart :"$caredepartward.code",
        firstdepart:"$badmoveward.code",
            display: {
        $cond: {
          if: { $ne: ["$badmoveward.code", ""] },
          then: "$badmoveward.code",
          else: {
            $cond: {
              if: { $ne: ["$_id.ward01", ""] },
              then: "$_id.ward01",
              else: "$caredepartward.code"
            }
          }
        }
      }
    }
}



    ])