db.getCollection("patientvisits").aggregate([
 { $lookup: { from: "referencevalues", localField: "entypeuid", foreignField: "_id", as: "entypeuid" } },
 { $unwind: { path: "$entypeuid", preserveNullAndEmptyArrays: true } },
 {
    $match:{
        "entypeuid.valuecode":"ENTYPE2"
    }
},
{
    $match:{
        visitid:"67-2I007518",
        medicaldischargedate:{$nin:[null,""]}
    }
},

    {
  $lookup: {
    from: 'patientbills',
    let: {
        visitid:"$_id",
    },
    pipeline: [
          {
        $match: {
          $expr: {
            $eq: ['$$visitid', '$patientvisituid']
          },
            iscancelled:false
        }
      },
      {
        $project: {
          net:"$totalbillamount"
        }
      }
    ],
    as: 'totalbill01'
  }
},
{
    $addFields:{
        sumtotal:{$sum:"$totalbill01.net"}
    }
},
{ $lookup: { from: "patientbills", localField: "_id", foreignField: "patientvisituid", as: "bill" } },
{ $unwind: { path: "$bill", preserveNullAndEmptyArrays: true } },
{
    $match:{
        "bill.iscancelled":{$nin:[true,null,""]}
    }
},
 { $unwind: { path: "$bill.patientbilleditems", preserveNullAndEmptyArrays: true } },
    {
  $lookup: {
    from: 'patientorders',
    let: {
        visitid:"$_id",
       patientorderitemuid :"$bill.patientbilleditems.patientorderitemuid"
    },
    pipeline: [
          {
        $match: {
          $expr: {
            $eq: ['$$visitid', '$patientvisituid']
          }
        }
      },
      { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
        {
        $match: {
          $expr: {
            $eq: ['$$patientorderitemuid', '$patientorderitems._id']
          }
        }
      },
      {
        $project: {
          orderdate:"$orderdate",
          tariff:"$patientorderitems.tariffuid",
          qty:"$patientorderitems.quantity",
        }
      }
    ],
    as: 'order'
  }
},
{
  $addFields: {
    orderdate: {$last:"$order.orderdate"},
    tariff: {$last:"$order.tariff"},
   order:{$last:"$order.qty"} 
  }
},
{
    $sort:{
        orderdate:1
    }
},
{
  $addFields: {
    adjustedDate: {
      $cond: {
        if: { $gte: [{ $hour: "$orderdate" }, 17] }, 
        then: { $add: ["$orderdate", 25200000] }, 
        else: "$orderdate"
      }
    }
  }
},
{
    $addFields:{
              dategroup: {
        $dateToString: {
          format: "%d/%m/%Y", 
          date: "$adjustedDate" 
        }
      }
    }
},
 { $lookup: { from: "tariffs", localField: "tariff", foreignField: "_id", as: "tariff" } },
 { $unwind: { path: "$tariff", preserveNullAndEmptyArrays: true } },
  { $lookup: { from: "billinggroups", localField: "tariff.billinggroupuid", foreignField: "_id", as: "billinggroupuid" } },
 { $unwind: { path: "$billinggroupuid", preserveNullAndEmptyArrays: true } },
{
    $sort:{
        "billinggroupuid.name":1
    }
},
 { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patients" } },
 { $unwind: { path: "$patients", preserveNullAndEmptyArrays: true } },



//{ $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "patientvisits" } },
// { $unwind: { path: "$patientvisits", preserveNullAndEmptyArrays: true } },
// { $lookup: { from: "referencevalues", localField: "patientvisits.entypeuid", foreignField: "_id", as: "entypeuid" } },
// { $unwind: { path: "$entypeuid", preserveNullAndEmptyArrays: true } },
//{
//    $match:{
//        "patientvisits.visitid":"67-2I007221",
//        "entypeuid.valuecode":"ENTYPE2"
//    }
//},
// { $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
// {
//     $sort:{
//         orderdate:1
//     }
// },
//    {
//  $lookup: {
//    from: 'patientchargecodes',
//    let: {
//        visitid:"$patientvisituid",
//      orderitemuid: '$patientorderitems._id'
//    },
//    pipeline: [
//      {
//        $match: {
//          $expr: {
//            $eq: ['$$visitid', '$patientvisituid']
//          },
//        }
//      },
//    { $unwind: { path: "$chargecodes", preserveNullAndEmptyArrays: true } },
//      {
//        $match: {
//          $expr: {
//            $eq: ['$$orderitemuid', '$chargecodes.patientorderitemuid']
//          }
//        }
//      },
//      {
//        $project: {
//          _id: 0,
//          BillQTY: "$chargecodes.orgquantity",
//          Billtotalprice: "$chargecodes.netamount",
//          patientbilluid:"$chargecodes.patientbilluid"
//        }
//      }
//    ],
//    as: 'billDetail'
//  }
//},
//             { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "statusorder" } },
//            { $unwind: { path: "$statusorder", preserveNullAndEmptyArrays: true } },
//            {
//                $sort:{
//                    orderdate:1
//                }
//            },
//{
//    $match:{
//        "statusorder.valuecode":{$nin:["ORDSTS4"]}
//    }
//},
//{ $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "patientuid" } },
// { $unwind: { path: "$patientuid", preserveNullAndEmptyArrays: true } },
// { $unwind: { path: "$billDetail", preserveNullAndEmptyArrays: true } },
// {
//     $match:{
//         billDetail:{$nin:[null]}
//     }
// },
// { $lookup: { from: "tariffs", localField: "patientorderitems.tariffuid", foreignField: "_id", as: "tariff" } },
// { $unwind: { path: "$tariff", preserveNullAndEmptyArrays: true } },
//  { $lookup: { from: "billinggroups", localField: "tariff.billinggroupuid", foreignField: "_id", as: "billinggroupuid" } },
// { $unwind: { path: "$billinggroupuid", preserveNullAndEmptyArrays: true } },
//{
//    $sort:{
//        "billinggroupuid.name":1
//    }
//},
//{
//  $addFields: {
//    adjustedDate: {
//      $cond: {
//        if: { $gte: [{ $hour: "$orderdate" }, 17] }, 
//        then: { $add: ["$orderdate", 25200000] }, 
//        else: "$orderdate"
//      }
//    }
//  }
//},
//{
//    $addFields:{
//              dategroup: {
//        $dateToString: {
//          format: "%d/%m/%Y", 
//          date: "$adjustedDate" 
//        }
//      }
//    }
//},
//{
//    $project:{
//        itemname:"$patientorderitems.orderitemname",
//        itemQTY:"$patientorderitems.quantity",
//        totalprice:"$patientorderitems.totalprice",
//        billgroup:"$billinggroupuid.name",
//       HN:"$patientuid.mrn",
//       fname:"$patientuid.firstname",
//       lname:"$patientuid.lastname",
//       VN:"$patientvisits.visitid",
//     orderdate:"$orderdate",
//  adjustedDate:"$adjustedDate",
//        dategroup:1,
//     billDetail:1,
//   ordernumber:1,
//  itemid:"$patientorderitems._id",
//    visitid:"$patientvisits._id"
//    }
//},
//{
//    $sort:{
//        dategroup:1
//    }
//},
//
//    {
//  $lookup: {
//    from: 'patientbills',
//    let: {
//      billid: '$billDetail.patientbilluid',
//      orderitemuid:"$itemid"
//    },
//    pipeline: [
//      {
//        $match: {
//          $expr: {
//            $eq: ['$$billid', '$_id']
//          },
//                  iscancelled:false
//        }
//      },
// { $unwind: { path: "$patientbilleditems", preserveNullAndEmptyArrays: true } },
//      {
//        $match: {
//          $expr: {
//            $eq: ['$$orderitemuid', '$patientbilleditems.patientorderitemuid']
//          }
//        }
//      },
//      {
//        $project: {
//          _id: 0,
// discount: { $add: ["$patientbilleditems.payordiscount", "$patientbilleditems.specialdiscount"] }, 
// Billtotalprice: "$patientbilleditems.netamount",
// roundoff:"$roundoff",
//        }
//      }
//    ],
//    as: 'billDetail2'
//  }
//},
// {
//    $addFields: {
//      totalPrice2: {
//        $cond: {
//           if: { $eq: [{ $size: "$billDetail2" }, 0] },   
//          then: "", 
//          else: { $last: "$billDetail2.Billtotalprice" } 
//        }
//      },
//    }
//  },
// {
//     $match:{
//         totalPrice2:{$nin:[""]}
//     }
// } ,
//    {
//  $lookup: {
//    from: 'patientbills',
//    let: {
//        visitid:"$visitid",
//    },
//    pipeline: [
//          {
//        $match: {
//          $expr: {
//            $eq: ['$$visitid', '$patientvisituid']
//          },
//            iscancelled:false
//        }
//      },
//      {
//        $project: {
//          net:"$totalbillamount"
//        }
//      }
//    ],
//    as: 'totalbill01'
//  }
//},
//{
//    $addFields:{
//        sumtotal:{$sum:"$totalbill01.net"}
//    }
//}





])