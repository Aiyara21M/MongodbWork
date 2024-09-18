db.getCollection("consultrequests").aggregate([

{
    $match:{
        referraldate:{
            $gte:ISODate("2024-07-16T00:00:00.000+07:00"),
            $lte:ISODate("2024-07-16T23:59:59.999+07:00"),
        },
        referringdepartmentuid:ObjectId("63d9e0e0f458c20014126a4d"),
        "isactive" : true
    }
},
   { $lookup: { from: 'departments', localField: 'referredto', foreignField: '_id', as: 'depart2' } },
    { $unwind: { path: '$depart2', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'departments', localField: 'referringdepartmentuid', foreignField: '_id', as: 'depart' } },
    { $unwind: { path: '$depart', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'referringcareprovideruid', foreignField: '_id', as: 'doc' } },
    { $unwind: { path: '$doc', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'referredtocareprovider', foreignField: '_id', as: 'doc2' } },
    { $unwind: { path: '$doc2', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'consultreasons', foreignField: '_id', as: 'reason' } },
    { $unwind: { path: '$reason', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'patients', localField: 'patientuid', foreignField: '_id', as: 'patientuid' } },
    { $unwind: { path: '$patientuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'patientvisits', localField: 'patientvisituid', foreignField: '_id', as: 'patientvisituid' } },
    { $unwind: { path: '$patientvisituid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'patientuid.titleuid', foreignField: '_id', as: 'title' } },
    { $unwind: { path: '$title', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues",localField: "patientvisituid.entypeuid",foreignField: "_id",as: "type"}},
    { $unwind: { path: "$type",preserveNullAndEmptyArrays: true}},
    {
        $match:{
            "type.valuecode" : "ENTYPE1"
        }
    },
{
    $project:{
        referraldate:1,
        title:"$title.valuedescription",
        firstname:"$patientuid.firstname",
        lastname:{$ifNull:["$patientuid.lastname",""]},
        mrn:"$patientuid.mrn",
        comments:1,
        depart:"$depart.name",
        depart2:"$depart2.name",
        doc:{$ifNull:["$doc.name",""]},
        doc2:{$ifNull:["$doc2.name",""]},
        reason:"$reason.valuedescription",
        visitid:"$patientvisituid.visitid"
    }
}

])
