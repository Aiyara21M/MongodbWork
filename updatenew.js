db.getCollection("patients").aggregate([
                {
                    $match:{
                        mrn:"45-049602"
                    }
                },
  { $lookup: { from: "patientvisits", localField: "_id", foreignField: "patientuid", as: "patientvisits" } },
  { $unwind: { path: "$patientvisits", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patientorders", localField: "patientvisits._id", foreignField: "patientvisituid", as: "order" } },
  { $unwind: { path: "$order", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "order.ordertypeuid", foreignField: "_id", as: "type" } },
  { $unwind: { path: "$type", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$order.patientorderitems", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: "referencevalues", localField: "order.patientorderitems.statusuid", foreignField: "_id", as: "status" } },
  {
      $match:{
          "order.patientorderitems.ordercattype":{ $regex: /^medicin/i },
          "type.valuecode":{$in:["ORDTYP1","ORDTYP2","ORDTYP3"]},
      }
  },
       { $lookup: { from: "users",localField: "order.patientorderitems.careprovideruid",foreignField: "_id",as: "datadoc"}},
     { $unwind: { path: "$datadoc",preserveNullAndEmptyArrays: true}},
            { $lookup: { from: "frequencies",localField: "order.patientorderitems.frequencyuid",foreignField: "_id",as: "fre"}},
     { $unwind: { path: "$fre",preserveNullAndEmptyArrays: true}},
         { $unwind: { path: "$status",preserveNullAndEmptyArrays: true}},
{
    $group:{
        _id:{
      HN: "$mrn",
      firstname: "$firstname",
      lastname: "$lastname",
      VN: "$patientvisits.visitid",
      ordername: "$order.patientorderitems.orderitemname",
      codeorder: "$order.patientorderitems.chargecode",
      frq: "$fre.name",
      codedoc: "$datadoc.code",
      namedoc: "$datadoc.name",
      type: "$type.valuedescription",
      date:"$order.orderdate",
     qty:"$order.patientorderitems.quantity"
        },
              status: {"$last":"$status"},
    }
},
{
    $match:{
        "status.valuecode":{$nin:["ORDSTS5","ORDSTS4"]}
    }
},
  {
    $project: {
      HN: "$_id.HN",
      firstname: "$_id.firstname",
      lastname: "$_id.lastname",
      VN: "$_id.VN",
      ordername: "$_id.ordername",
      codeorder: "$_id.codeorder",
      frq: "$_id.frq",
      codedoc: "$_id.codedoc",
      namedoc: "$_id.namedoc",
      type: "$_id.type",
        date:"$_id.date",
        qty:"$_id.qty"
    }
  },
  {
      $sort:{
           codedoc:-1,
          date:-1,
      }
  },
  {
      $match:{
 "ordername":{$nin:[/^NSS/i ,/^D-5-W/i,/^D-10/i,/^ADMIN/i]}
      }
  },
//  {
//    $group: {
//      _id: {
//          namedoc:"$namedoc", 
//          codedoc:"$codedoc"},
//         HN: {"$first":"$HN"},
//          firstname:{"$first":"$firstname"} ,
//          lastname: {"$first":"$lastname"},
//      data: {
//        $push: {
//          VN: "$VN",
//          ordername: "$ordername",
//          codeorder: "$codeorder",
//          frq: "$frq",
//          status: "$status",
//          type: "$type",
//            date:"$date",
//            qty:"$qty"
//        }
//      }
//    }
//  },
//  {
//    $unwind: "$data"
//  },
//  {
//    $group: {
//      _id: {
//        namedoc: "$_id.namedoc",
//        codedoc:"$_id.codedoc",
//        type: "$data.type"
//      },
//          HN: {"$first":"$HN"},
//          firstname:{"$first":"$firstname"} ,
//          lastname: {"$first":"$lastname"},
//      items: {
//        $push: {
//          VN: "$data.VN",
//          ordername: "$data.ordername",
//          codeorder: "$data.codeorder",
//          frq: "$data.frq",
//          codedoc: "$data.codedoc",
//          date:"$data.date",
//          qty:"$data.qty"
//        }
//      }
//    }
//  },
//  {
//     $sort:{
//         "_id.type":1
//     } 
//  },
//  {
//    $group: {
//      _id: {
//        namedoc: "$_id.namedoc",
//        codedoc:"$_id.codedoc",
//      },
//          HN: {"$first":"$HN"},
//          firstname:{"$first":"$firstname"} ,
//          lastname: {"$first":"$lastname"},
//          types: {
//          $push: {
//          type: "$_id.type",
//          data: "$items"
//        }
//      },
//    }
//  },
//  {
//      $sort:{
//          _id:1
//      }
//  },
//  {
//      $group:{
//           _id:"$HN",
//          firstname:{"$first":"$firstname"} ,
//          lastname: {"$first":"$lastname"},
//          doc:{$push:{
//            namedoc : "$_id.namedoc",
//            codedoc:"$_id.codedoc",
//             types:"$types"
//          }
//          }
//      },
//  },

])
