db.getCollection("patientorders").aggregate([
    { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
    { $match: {
            "patientorderitems.startdate": {
	        $gte: ISODate("2024-05-23T00:00:00.000+07:00"),
            $lte : ISODate("2024-05-23T23:59:59.999+07:00") 
            },
              "patientorderitems.patientpackageuid":{$ne:null}
        }},
    { $lookup: { from: "ordercategories", localField: "patientorderitems.ordersubcatuid", foreignField: "_id", as: "subcat" } },
    { $unwind: { path: "$subcat", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "departments", localField: "orderdepartmentuid", foreignField: "_id", as: "department" } },
    { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
        {
        $match: {
        "status.valuedescription": { $ne: "Cancelled" },
        "subcat.code":{ $regex: /^DF/i },
        }
    },
    {
        $lookup: {
            from: 'patientpackages',
            let: {
                pkgid: '$patientorderitems.patientpackageuid',
                itemid: '$patientorderitems.orderitemuid'
            },
            pipeline: [
                            {
                    $match: { $expr: 
                                { $eq: ['$$pkgid', '$_id'] },
                                             }
                },
    { $unwind: { path: "$orderitems", preserveNullAndEmptyArrays: true } },
                {
                    $match: { $expr: 
                                { $eq: ['$$itemid', '$orderitems.orderitemuid'] }
                                          }
                },
                {$project: {
                        _id:0,
                       "orderitems":"$orderitems.packageitemprice",
                       "pkgname":"$packagename",
                       "totalprice":"$netamount"
                    }}
            ],
            as: 'PKG001'
        }
    },
      { $unwind: { path: "$PKG001", preserveNullAndEmptyArrays: true } },
          { $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "VN" } },
    { $unwind: { path: "$VN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "careprovideruid", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "patientorderitems.careprovideruid", foreignField: "_id", as: "doc2" } },
    { $unwind: { path: "$doc2", preserveNullAndEmptyArrays: true } },
    {
        $lookup: {
            from: 'patientvisits',
            let: {
                visitpayor: '$patientorderitems.patientvisitpayoruid',
                visitid: '$patientvisituid'
            },
            pipeline: [
                            {
                    $match: { $expr: 
                                { $eq: ['$$visitid', '$_id'] },
                                             }
                },
                    { $unwind: { path: "$visitpayors", preserveNullAndEmptyArrays: true } },
                                           {
                    $match: { $expr: 
                                { $eq: ['$$visitpayor', '$visitpayors._id'] },
                                             }
                },
                {$project: {
                        _id:0,
                        payor:"$visitpayors.payoruid"
                    }}
            ],
            as: 'payor'
        }
    },
{ $unwind: { path: "$payor", preserveNullAndEmptyArrays: true } },
 { $lookup: { from: "payors", localField: "payor.payor", foreignField: "_id", as: "payor" } },
    { $unwind: { path: "$payor", preserveNullAndEmptyArrays: true } },

            {
        $project:{
             _id:"$_id",
            "codedoc": { "$ifNull": ["$doc.code", "$doc2.code"] },
            "namedoc": { "$ifNull": ["$doc.name", "$doc2.name"] },
            "itemname":"$patientorderitems.orderitemname",
           "payor":"$payor.name",
            "VN":"$VN.visitid",
            "HN":"$HN.mrn",
            "fname":"$HN.firstname",
            "lname":{ "$ifNull": ["$HN.lastname", ""] },
            "totalprice":{ "$ifNull": ["$PKG001.totalprice", null] },
            "startdate":"$patientorderitems.startdate",
            "package":"$PKG001.pkgname",
               "department":"$department.name",
               "itemprice": { "$ifNull": ["$PKG001.orderitems", null] }
        }
    },
{
    $sort:{
        startdate:1
    }
}


])
