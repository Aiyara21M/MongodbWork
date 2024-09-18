db.getCollection("patientbills").aggregate([
 {
     $match: {
        billdate:{
         $gte : ISODate("2024-04-01T00:00:00.000+07:00"),
        $lte : ISODate("2024-04-01T23:59:59.999+07:00") 
         },
         "isrefund" : false,
         "iscancelled" : false
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
                {$project: {
                        _id:0,
                        comments: "$patientorderitems.comments",
                    }}
            ],
            as: 'comments'
        }
    },
     { $unwind: { path: "$comments",preserveNullAndEmptyArrays: true}},
       
     { $lookup: { from: "patients",localField: "patientuid",foreignField: "_id",as: "patientuid"}},
     { $unwind: { path: "$patientuid",preserveNullAndEmptyArrays: true}},
     
     { $lookup: { from: "patientvisits",localField: "patientvisituid",foreignField: "_id",as: "patientvisituid"}},
     { $unwind: { path: "$patientvisituid",preserveNullAndEmptyArrays: true}},

     { $lookup: { from: "users",localField: "patientbilleditems.careprovideruid",foreignField: "_id",as: "datadoc"}},
     { $unwind: { path: "$datadoc",preserveNullAndEmptyArrays: true}},
     
     { $lookup: { from: "orderitems",localField: "patientbilleditems.orderitemuid",foreignField: "_id",as: "dataitem"}},
     { $unwind: { path: "$dataitem",preserveNullAndEmptyArrays: true}},

     { $lookup: { from: "billinggroups",localField: "patientbilleditems.billinggroupuid",foreignField: "_id",as: "billG"}},
     { $unwind: { path: "$billG",preserveNullAndEmptyArrays: true}},
      
     { $lookup: { from: "billinggroups",localField: "patientbilleditems.billingsubgroupuid",foreignField: "_id",as: "billG2"}},
     { $unwind: { path: "$billG2",preserveNullAndEmptyArrays: true}},
        
     { $lookup: { from: "users",localField: "patientbilleditems.careprovideruid",foreignField: "_id",as: "datadoc"}},
     { $unwind: { path: "$datadoc",preserveNullAndEmptyArrays: true}},

     { $lookup: { from: "referencevalues",localField: "patientvisituid.entypeuid",foreignField: "_id",as: "status"}},
     { $unwind: { path: "$status",preserveNullAndEmptyArrays: true}},
     
     { $lookup: { from: "departments",localField: "patientvisituid.visitjourneys.0.departmentuid",foreignField: "_id",as: "depart"}},
     { $unwind: { path: "$depart",preserveNullAndEmptyArrays: true}},
     
     { $lookup: { from: "payors",localField: "payoruid",foreignField: "_id",as: "payor"}},
     { $unwind: { path: "$payor",preserveNullAndEmptyArrays: true}},
     
     { $lookup: { from: "payoragreements",localField: "payoragreementuid",foreignField: "_id",as: "payorar"}},
     { $unwind: { path: "$payorar",preserveNullAndEmptyArrays: true}},
     {
     $match: {
         "status.relatedvalue":"OPD",
         "depart.code":"DEN"
     }
     }, 
       {$group:{
           _id:{
           _id:"$id",
           VN:"$patientvisituid.visitid",
           HN:"$patientuid.mrn",
           Name : "$patientuid.firstname",
           lname: "$patientuid.lastname",
           billnumber:"$sequencenumber",
           codeitem:"$dataitem.code",
           nameitem:"$patientbilleditems.orderitemname",
           Qty:"$patientbilleditems.quantity",
           doccode:"$datadoc.code",
           docname:"$datadoc.name",
           sumb:"$patientbilleditems.unitprice",
           discount:{ $sum: ["$patientbilleditems.payordiscount", "$patientbilleditems.specialdiscount"] },
           totalbill:"$patientbilleditems.netamount",
           billGroup:"$billG.name",
           SubGroup:"$billG2.name",
           ordernumber:"$patientbilleditems.ordernumber",
           chargedate:"$patientbilleditems.chargedate",
           payor:"$payor.name",
           payorar:"$payorar.name"
           },
           comments : {"$last":"$comments.comments"}
       }},
           {$project:{
           _id:"$_id._id",
           VN:"$_id.VN",
           HN:"$_id.HN",
           Name : "$_id.Name",
           lname: "$_id.lname",
           billnumber:"$_id.billnumber",
           codeitem:"$_id.codeitem",
           nameitem:"$_id.nameitem",
           Qty:"$_id.Qty",
           doccode:"$_id.doccode",
           docname:"$_id.docname",
           sumb:"$_id.sumb",
           discount:"$_id.discount",
           totalbill:"$_id.totalbill",
           billGroup:"$_id.billGroup",
           SubGroup:"$_id.SubGroup",
           comments:"$comments",
           ordernumber:"$_id.ordernumber",
           chargedate:"$_id.chargedate",
           payor:"$_id.payor",
           payorar:"$_id.payorar"
       }},
       {
           $sort:{
               billnumber:1
           }
       },


])