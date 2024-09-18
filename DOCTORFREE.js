db.getCollection("patientorders").aggregate([
  { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
    { $match: {
            "patientorderitems.startdate": {
                $gte: ISODate("2024-04-01T00:00:00.000+07:00"),
                $lte: ISODate("2024-04-01T23:59:59.999+07:00")
            },
              "patientorderitems.patientpackageuid":{$ne:null}
        }},
        { $lookup: { from: "patientpackages", localField: "patientorderitems.patientpackageuid", foreignField: "_id", as: "package" } },
    { $unwind: { path: "$package", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "ordercategories", localField: "patientorderitems.ordersubcatuid", foreignField: "_id", as: "subcat" } },
    { $unwind: { path: "$subcat", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "ordercategories", localField: "patientorderitems.ordersubcatuid", foreignField: "_id", as: "subcat" } },
    { $unwind: { path: "$subcat", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "VN" } },
    { $unwind: { path: "$VN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "orderitems", localField: "patientorderitems.orderitemuid", foreignField: "_id", as: "item" } },
    { $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "careprovideruid", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "patientorderitems.careprovideruid", foreignField: "_id", as: "doc2" } },
    { $unwind: { path: "$doc2", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "departments", localField: "orderdepartmentuid", foreignField: "_id", as: "department" } },
    { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
    {
        $match: {
        "status.valuedescription": { $ne: "Cancelled" },
        "subcat.name":{ $regex: /^DF in order set/i },
        }
    },
   
    {
        $lookup: {
            from: 'patientpackages',
            let: {
                pkgid: '$patientorderitems.patientpackageuid',
                itemid: '$item._id'
            },
            pipeline: [
    { $unwind: { path: "$orderitems", preserveNullAndEmptyArrays: true } },
                {
                    $match: { $expr: {$and: [
                                { $eq: ['$$pkgid', '$_id'] },
                                { $eq: ['$$itemid', '$orderitems.orderitemuid'] }
                                            ]  }  }
                },
                {$project: {
                        _id:0,
                       "orderitems":"$orderitems.packageitemprice",
                    }}
            ],
            as: 'PKG001'
        }
    },
      { $unwind: { path: "$PKG001", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$VN.visitpayors", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "payors", localField: "VN.visitpayors.payoruid", foreignField: "_id", as: "payor" } },
    { $unwind: { path: "$payor", preserveNullAndEmptyArrays: true } },
    {
        $group:{
             _id: {
             _id:"$_id",
            "codedoc": { "$ifNull": ["$doc.code", "$doc2.code"] },
            "namedoc": { "$ifNull": ["$doc2.name", "$doc2.name"] },
            "itemname":"$item.name",
            "VN":"$VN.visitid",
            "HN":"$HN.mrn",
            "fname":"$HN.firstname",
            "lname":"$HN.lastname",
            "totalprice":"$package.netamount",
            "startdate":"$patientorderitems.startdate",
            "package":"$package.packagename",
            "code":"$item.code",
               "department":"$department.name",
               },
               "itemprice":{$max:"$PKG001.orderitems" },
  				"payor":{"$first":"$payor.name"},
 payor2: {
            $push: {
                $cond: {
                    if: { $eq: ["$VN.visitpayors._id", "$patientorderitems.patientvisitpayoruid"] },
                    then: "$VN.visitpayors.payoruid",
                       else:   "$$REMOVE" 
                }
            }
        },
        }},
       { $unwind: { path: "$payor2", preserveNullAndEmptyArrays: true } },
       { $lookup: { from: "payors", localField: "payor2", foreignField: "_id", as: "payor2" } },
    { $unwind: { path: "$payor2", preserveNullAndEmptyArrays: true } },
            {
        $project:{
             _id:"$_id._id",
            "codedoc": "$_id.codedoc",
            "namedoc": "$_id.namedoc",
            "itemname":"$_id.itemname",
            "payor":"$payor",
             "payor2":"$payor2.name",
            "VN":"$_id.VN",
            "HN":"$_id.HN",
            "fname":"$_id.fname",
            "lname":"$_id.lname",
            "totalprice":"$_id.totalprice",
            "startdate":"$_id.startdate",
            "package":"$_id.package",
            "code":"$_id.code",
               "department":"$_id.department",
               "itemprice": { "$ifNull": ["$itemprice", null] }
        }
    }
,{
    $sort:{
        startdate:1
    }
}
 




])
