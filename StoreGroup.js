db.getCollection("stockledgers").aggregate([
{$match:{
    statusflag:"A",
    storeuid:{$in: [ObjectId("64ad01c93337c300758c0a9b"),ObjectId("636cb9220c42a700123f6317")]}
    }},
      { $lookup: { from: "inventorystores", localField: "storeuid", foreignField: "_id", as: "stores" } },
      { $unwind: { path: "$stores", preserveNullAndEmptyArrays: true } },
      { $lookup: { from: "referencevalues", localField: "quantityuom", foreignField: "_id", as: "quantityuom" } },
      { $unwind: { path: "$quantityuom", preserveNullAndEmptyArrays: true } },
      {
        $addFields: {
          isitemmasteridrequestmatched: {
            $cond: [
              { $eq : [ { $size: { $ifNull : [ "$requestitemmasteruids", [] ] } }, 0 ] },
              true,
              { $in: [ "$itemmasteruid", "$requestitemmasteruids" ] }
            ]
          }
        },
      },
      { $match: { isitemmasteridrequestmatched: true } },
      { $lookup: { from: "itemmasters", localField: "itemmasteruid", foreignField: "_id", as: "itemmasters" } },
      { $unwind: { path: "$itemmasters", preserveNullAndEmptyArrays: true } },
      	{ $match: { "itemmasters.handlingstores.storeuid": {$in: [ObjectId("64ad01c93337c300758c0a9b"),ObjectId("636cb9220c42a700123f6317")]} } },

      {
        $addFields: {
          isitemtypeidrequestmatched: {
            $cond: [
              { $eq : [ { $size: { $ifNull: [ "$requestuids", [] ] } }, 0 ] },
              true,
              { $in: [ "$itemmasters.itemtypeuid", "$requestuids" ] }
            ]
          }
        },
      },
            { $match: { isitemtypeidrequestmatched: true } },
      { $lookup: { from: "referencevalues", localField: "itemmasters.itemtypeuid", foreignField: "_id", as: "itemtype" } },
      { $unwind: { path: "$itemtype", preserveNullAndEmptyArrays: true } },
      { $lookup: { from: "weightedaveragecosts", localField: "itemmasteruid", foreignField: "itemmasteruid", as: "wac" } },
         {
        $group: {
          _id: {
            "batchid": "$batchid",
            "expirydate": "$expirydate",
            "storeuid": "$stores._id",
            "storecode": "$stores.code",
            "storename": "$stores.name",
            "itemname": "$itemmasters.name",
            "itemcode": "$itemmasters.code",
            "orguid": "$orguid",
            "itemtypeuid": "$itemtype.valuedescription",
            "weightedaveragecost": { $arrayElemAt: [ { $reverseArray: "$wac.weightedavgcost" }, 0 ] },
            "handlingstore": {
              $filter: {
                input: "$itemmasters.handlingstores",
                as: "store",
                cond: {
                  $and: [
                    { $eq: [ "$$store.storeuid", "$stores._id" ] }
                  ]
                }
              }
            }, 
          },
          quantity: { $sum: "$quantity" },
          quantityuom: { $first: "$quantityuom.valuedescription" }
        }
      },
            {
        $addFields: {
          "wac": "$_id.weightedaveragecost.weightedavgcost"
        }
      },
            {
        $project:{
          "batchid": "$_id.batchid",
          "expirydate": "$_id.expirydate",
          "storeuid": "$_id.storeuid",
          "storecode": "$_id.storecode",
          "storename": "$_id.storename",
          "itemname": "$_id.itemname",
          "itemcode": "$_id.itemcode",
          "orguid": "$_id.orguid",
          "itemtypeuid": "$_id.itemtypeuid",
          "weightedaveragecost": "$_id.weightedaveragecost",
          "handlingstore": "$_id.handlingstore", 
          "quantity": 1,
          "quantityuom": 1,
          "wac": 1
        }
      },
         {
        $project: {
          "batchid": 1,
          "expirydate": 1,
          "storeuid": 1,
          "storecode": 1,
          "storename": 1,
          "itemname": 1,
          "itemcode": 1,
          "orguid": 1,
          "itemtypeuid": 1,
          "weightedaveragecost": 1,
          "handlingstore": 1, 
          "quantity": 1,
          "quantityuom": 1,
          "wac": 1,
          "binuid": { $arrayElemAt: [ "$handlingstore.binuid", 0 ] },
        }
      },
            { $lookup: { from: "inventorystores", localField: "binuid", foreignField: "storebins._id", as: "storebinuid" } },
      { $unwind: { path: "$storebinuid" , preserveNullAndEmptyArrays: true } },
          {
        $project: {
          "batchid": 1,
          "expirydate": 1,
          "storeuid": 1,
          "storecode": 1,
          "storename": 1,
          "itemname": 1,
          "itemcode": 1,
          "orguid": 1,
          "itemtypeuid": 1,
          "weightedaveragecost": 1,
          "handlingstore": 1, 
          "quantity": 1,
          "quantityuom": 1,
          "wac": 1,
          "storebinfilter": { $arrayElemAt: [ 
            { $filter: {
                input: "$storebinuid.storebins",
                as: "storebins",
                cond: {
                  $and: [
                    { $eq: ["$$storebins._id", "$binuid"] }
                  ]
                }
              }
            } ,0 ]
          },
          "searchcriteria": {
            $cond: [
              {
                $and: [
                  { $ne: [ "", null ] },
                  { $ne: [ "", "" ] },
                ]
              },
             "",
              3650
            ]
          }
        }
      },
        {
        $project:{
          "batchid": 1,
          "expirydate": 1,
          "storeuid": 1,
          "storecode": 1,
          "storename": 1,
          "itemname": 1,
          "itemcode": 1,
          "orguid": 1,
          "itemtypeuid": 1,
          "weightedaveragecost": 1,
          "handlingstore": 1, 
          "quantity": 1,
          "quantityuom": 1,
          "wac": 1,
          "storebinname": "$storebinfilter.name",
          "searchcriteria": { $add: [ ISODate() , { $multiply: [ {$toInt: "$searchcriteria"}  , 86400000 ] } ] },
          "withzero": { $gt: [ "$quantity", { $toInt: "0" } ] }
        }
      },
         {
        $addFields: {
          "isFindItem": {
            $cond: [
              { $and: [
                  { $gte: [ "$expirydate", ISODate() ] },
                  { $lte: [ "$expirydate", "$searchcriteria" ] },
                ] 
              },
              true,
              false
            ] 
          }
        }
      },
      { 
        $match: { 
          isFindItem : true, 
          withzero: true,
        }
      },
{
    $group:{
        _id:"$itemcode",
        itemname:{"$first":"$itemname"},
        storename:{"$first":"$storename"},
        storecode:{"$first":"$storecode"},
        quantityuom:{"$first":"$quantityuom"},
        quantity:{"$sum":"$quantity"},
        weightedaveragecost:{"$first":"$_id.weightedaveragecost"},
        itemtypeuid:{"$first":"$_id.itemtypeuid"},
        allstore:{$first:"Pharmacy Main(คลัง 99) + Pharmacy 99 New"}
    }
},
{
    $match:{
      itemtypeuid:"MEDICINE"  
    }
},
{
    $sort:{
        itemname:1
    }
}


])