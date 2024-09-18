db.getCollection("patientvisits").aggregate([
{
   $match:{
startdate:{
                $gte: ISODate("2024-04-01T00:00:00.000+07:00"),
                $lte: ISODate("2024-04-01T23:59:59.999+07:00")
            },
//       visitid:"66-2I000001"
   } 
},
    { $lookup: { from: "patientorders", localField: "_id", foreignField: "patientvisituid", as: "order" } },
    { $unwind: { path: "$order", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "departments", localField: "order.userdepartmentuid", foreignField: "_id", as: "department" } },
    { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    { $match: {
    "department.code":{$in:["ORD","ORDP","ANE","ANEP"]},
    }},
{ $unwind: { path: "$order.patientorderitems", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "referencevalues", localField: "order.patientorderitems.statusuid", foreignField: "_id", as: "status" } },
        { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
        { $match: {
    "status.valuecode": { $nin: ["ORDSTS5","ORDSTS4"] },
    }},
    {
        $project:{
            visitid:1,
            fname:"$HN.firstname",
            lname:"$HN.lastname",
            ordername:"$order.patientorderitems.orderitemname",
            date:"$order.orderdate",
            qty:"$order.patientorderitems.quantity",
            totoprice:"$order.patientorderitems.totalprice",
            HN:"$HN.mrn"
        }
    }
        ,{
            $sort:{
                date:1
            }
        }
        
        
        ])
        
        