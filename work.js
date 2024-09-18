db.getCollection("patientvisits").aggregate([
        					{ 
							$match: { statusflag: 'A',  _id :ObjectId("66a301bfb79c850001ab776d") }
							},
{ $unwind: "$visitcareproviders" },
    { $unwind: "$visitpayors" },
    { $match: { statusflag: "A" } },
    { $lookup: { from: 'departments', localField: 'visitcareproviders.departmentuid', foreignField: '_id', as: 'visitcareproviders.departmentuid' } },
    { $unwind: { path: '$visitcareproviders.departmentuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'visitcareproviders.careprovideruid', foreignField: '_id', as: 'visitcareproviders.careprovideruid' } },
    { $unwind: { path: '$visitcareproviders.careprovideruid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'visitcareproviders.statusuid', foreignField: '_id', as: 'visitcareproviders.statusuid' } },
    { $unwind: { path: '$visitcareproviders.statusuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'payors', localField: 'visitpayors.payoruid', foreignField: '_id', as: 'visitpayors.payoruid' } },
    { $unwind: { path: '$visitpayors.payoruid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'payoragreements', localField: 'visitpayors.payoragreementuid', foreignField: '_id', as: 'visitpayors.payoragreementuid' } },
    { $unwind: { path: '$visitpayors.payoragreementuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'tpas', localField: 'visitpayors.tpauid', foreignField: '_id', as: 'visitpayors.tpauid' } },
    { $unwind: { path: '$visitpayors.tpauid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'visitstatusuid', foreignField: '_id', as: 'visitstatusuid' } },
    { $unwind: { path: '$visitstatusuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'patients', localField: 'patientuid', foreignField: '_id', as: 'patientuid' } },
    { $unwind: { path: '$patientuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'entypeuid', foreignField: '_id', as: 'entypeuid' } },
    { $unwind: { path: '$entypeuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referringorgs', localField: 'refererdetail.referringorguid', foreignField: '_id', as: 'refererdetail.referringorguid' } },
    { $lookup: { from: 'referencevalues', localField: 'patientuid.titleuid', foreignField: '_id', as: 'patientuid.titleuid' } },
    { $unwind: { path: '$patientuid.titleuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'referencevalues', localField: 'patientuid.genderuid', foreignField: '_id', as: 'patientuid.genderuid' } },
    { $unwind: { path: '$patientuid.genderuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'patientimages', localField: 'patientuid.patientimageuid', foreignField: '_id', as: 'patientuid.patientimageuid' } },
    { $unwind: { path: '$patientuid.patientimageuid', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'users', localField: 'modifiedby', foreignField: '_id', as: 'modifiedby' } },
    { $unwind: { path: '$modifiedby', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'allergies', localField: 'patientuid._id', foreignField: 'patientuid', as: 'allergies' } },
    { $unwind: { path: '$allergies', preserveNullAndEmptyArrays: true } },
{ $lookup: { from: 'referencevalues', localField: 'patientuid.maritalstatusuid', foreignField: '_id', as: 'patientuid.maritalstatusuid' } },
    { $unwind: { path: '$patientuid.maritalstatusuid', preserveNullAndEmptyArrays: true } },
  { $lookup: { from: 'patients', localField: 'patientuid.occupationuid ', foreignField: '_id', as: 'patientuid.occupationuid' } },
  { $unwind: { path: '$occupationuid', preserveNullAndEmptyArrays: true } },
  { $lookup: { from: 'referencevalues', localField: 'patientuid.raceuid', foreignField: '_id', as: 'patientuid.raceuid' } },
    { $unwind: { path: '$patientuid.raceuid', preserveNullAndEmptyArrays: true } },
  { $lookup: { from: 'patients', localField: 'patientuid.religionuid', foreignField: '_id', as: 'patientuid.religionuid' } },
  { $unwind: { path: '$patientuid.religionuid', preserveNullAndEmptyArrays: true } },
  { $lookup: { from: 'patients', localField: 'patientuid.nationalityuid', foreignField: '_id', as: 'patientuid.nationalityuid' } },
  { $unwind: { path: '$patientuid.nationalityuid', preserveNullAndEmptyArrays: true } },
  { $lookup: { from: 'users', localField: 'createdby', foreignField: '_id', as: 'createdby' } },
    { $unwind: { path: '$createdby', preserveNullAndEmptyArrays: true } },
 
    {
        $group: {
            _id: {
                _id: "$_id",
                visitid: "$visitid",
                startdate: "$startdate",
                enddate: "$enddate",
                isreadmission: "$isreadmission",
                visitstatusuid: "$visitstatusuid.valuedescription",
                entypecode: '$entypeuid.valuecode',
                entypeuid: '$entypeuid.valuedescription',
                refererdetail: "$refererdetail",
                orguid: "$orguid",
                modifiedby:"$modifiedby.name",
                createdbyname:"$createdby.name",
                patient: {
                    "_id": "$patientuid._id",
                    "firstname": "$patientuid.firstname",
                    "middlename": "$patientuid.middlename",
                    "lastname": "$patientuid.lastname",
                    "mrn": "$patientuid.mrn",
                    "dateofbirth": "$patientuid.dateofbirth",
                    "genderuid": "$patientuid.genderuid.valuedescription",
                    "isdobestimated":  {$ifNull :["$patientuid.isdobestimated",false]},
                    "title": "$patientuid.titleuid.valuedescription",
                    "address": "$patientuid.address",
                    "contact": "$patientuid.contact",
 					"nationalid":"$patientuid.nationalid",
               "maritalstatusuid":"$patientuid.maritalstatusuid.valuedescription",
                    "occupationuid":"$patientuid.occupationuid.valuedescription",
                    "raceuid":"$patientuid.raceuid.valuedescription",
                    "religionuid":"$patientuid.religionuid.valuedescription",
                    "nationalityuid":"$patientuid.nationalityuid.valuedescription"
                },
                drugallergies: "$allergies.drugallergies",
                patientnok: "$patientnok",
                patientnokcontact: "$patientnokcontact"
            }, visitcareproviders: {
                $push: {
                    "_id": "$visitcareproviders._id",
                    "departmentcode": "$visitcareproviders.departmentuid.code",
                    "departmentname": "$visitcareproviders.departmentuid.name",
                    "careprovidercode": "$visitcareproviders.careprovideruid.code",
                    "careprovidername": "$visitcareproviders.careprovideruid.name",
                    "statusuid": "$visitcareproviders.statusuid.valuedescription",
                    "comments": "$visitcareproviders.comments",
                    "queuenumber": "$visitcareproviders.queuenumber"
                }
            }, visitpayors: {
                $push: {
                    "_id": "$visitpayors._id",
                    "orderofpreference": "$visitpayors.orderofpreference",
                    "payorcode": "$visitpayors.payoruid.code",
                    "payorname": "$visitpayors.payoruid.name",
                    "payoragreementcode": "$visitpayors.payoragreementuid.code",
                    "payoragreementname": "$visitpayors.payoragreementuid.name",
                    "tpacode": "$visitpayors.tpauid.code",
                    "tpaname": "$visitpayors.tpauid.name",
                    "policydetails": "$visitpayors.policydetails",
                    "comments": "$visitpayors.comments"
                }
            }
        }
    }, {
        $project: {
            '_id': 1,
            'visitcareproviders': 1,
            'visitpayors': 1
        }
    },
    { $lookup: { from: 'cchpis', localField: '_id._id', foreignField: 'patientvisituid', as: 'cchpis' } },
    { $unwind: { path: '$cchpis', preserveNullAndEmptyArrays: true } },
    { $lookup: { from: 'diagnoses', localField: '_id._id', foreignField: 'patientvisituid', as: 'diagnoses' } },
    { $unwind: { path: '$diagnoses', preserveNullAndEmptyArrays: true } },
    {
        $project: {
            '_id': 1,
            'visitcareproviders': 1,
            'visitpayors': 1,
            'cchpis': { $arrayElemAt: ["$cchpis.cchpis", 0] },
            'diagnosis': {
                $arrayElemAt: [{
                    $filter: {
                        input: "$diagnoses.diagnosis", as: "d", cond: { $eq: ["$$d.isprimary", true] }
                    }
                }, 0]
            },
          'primaryvisitpayors': {  $arrayElemAt: [ { $filter : { input : "$visitpayors", as: "d" , 
               cond: { $eq:["$$d.orderofpreference",1] }} },0]}
        }
    },
    { $lookup: { from: 'problems', localField: 'diagnosis.problemuid', foreignField: '_id', as: 'diagnosis' } },
    { $unwind: { path: '$diagnosis', preserveNullAndEmptyArrays: true } },
    {
        $project: {
            '_id': 1,
            'visitcareproviders': 1,
            'visitpayors': 1,
            'cchpis': 1,
            'primarydiagnosis': "$diagnosis.name",
            'primaryvisitpayors': "$primaryvisitpayors.payorname"
        }
    },
    { $sort: { _id: 1 } },     
    { $limit: 1} 

])
