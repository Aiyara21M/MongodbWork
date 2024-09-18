db.getCollection("patientorders").aggregate([
   	{ $unwind: { path: "$patientorderitems", preserveNullAndEmptyArrays: true } },
 	{ $lookup: { from: "tariffs", localField: "patientorderitems.tariffuid", foreignField: "_id", as: "tarif" } },
    { $unwind: { path: "$tarif", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: "billinggroups", localField: "tarif.billingsubgroupuid", foreignField: "_id", as: "billsubgroup" } },
    { $unwind: { path: "$billsubgroup", preserveNullAndEmptyArrays: true } },
     { $lookup: { from: "users", localField: "careprovideruid", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "users", localField: "patientorderitems.careprovideruid", foreignField: "_id", as: "doc2" } },
    { $unwind: { path: "$doc2", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "orderitems", localField: "patientorderitems.orderitemuid", foreignField: "_id", as: "item" } },
    { $unwind: { path: "$item", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "patientorderitems.statusuid", foreignField: "_id", as: "status" } },
    { $unwind: { path: "$status", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patientvisits", localField: "patientvisituid", foreignField: "_id", as: "VN" } },
    { $unwind: { path: "$VN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    
 	 {
 	     $match:{
           "billsubgroup.code":"01005VMED",
           "status.valuecode": { $nin: ["ORDSTS5","ORDSTS4"] },
 	     }
 	 }
 	 ,{
 	     $project:{
 	         orderdate:1,
 	         HN:"$HN.mrn",
 	         Fname:"$HN.firstname",
 	         Lname:"$HN.lastname",
 	         birthday:"$HN.dateofbirth",
 	         isdobestimated:"$HN.isdobestimated",
 	         VN:"$VN.visitid",
 	         codeitem:"$item.code",
 	         nameitem:"$item.name",
 	         qty:"$patientorderitems.quantity",
 	         codedoc:"$doc2.code",
 	         namedoc:"$doc2.name"
 	     }
 	 },
 	 {
 	    $match:{
 	        HN:"65-208432"
 	    } 
 	 }
])