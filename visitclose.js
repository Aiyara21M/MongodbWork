db.getCollection("patientvisits").aggregate([
{
    $match:{
        startdate:{
	      $gte: ISODate("2024-06-18T00:00:00.000+07:00"),
          $lte: ISODate("2024-06-18T23:59:59.999+07:00")
        },
        visitclosereason:{$nin:[null]},
    }
},
    { $lookup: { from: "patientbills", localField: "_id", foreignField: "patientvisituid", as: "Bill" } },
    { $unwind: { path: "$Bill", preserveNullAndEmptyArrays: true } },



{ $unwind: { path: "$visitcareproviders", preserveNullAndEmptyArrays: true } },
{ $unwind: { path: "$visitjourneys", preserveNullAndEmptyArrays: true } },
{
  $group: {
    _id: "$_id",
    visitid:{$first:"$visitid"},
    HN: { $first: "$patientuid" },
    visitclosereason: { $first: "$visitclosereason" },
    startdate: { $first: "$startdate" },
    doc:{$first:"$visitcareproviders.careprovideruid"},
    department: { $first: "$visitjourneys.departmentuid" },
    medicaldischargedate:{$first:"$medicaldischargedate"},
    bill:{$last:"$Bill.iscancelled"}
  }
},
{
    $match:{
        "bill":{$nin:[false]}
    }
},
    { $lookup: { from: "referencevalues", localField: "visitclosereason", foreignField: "_id", as: "closedetail" } },
    { $unwind: { path: "$closedetail", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "HN", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
        { $lookup: { from: "departments", localField: "department", foreignField: "_id", as: "department" } },
    { $unwind: { path: "$department", preserveNullAndEmptyArrays: true } },
            { $lookup: { from: "users", localField: "doc", foreignField: "_id", as: "doc" } },
    { $unwind: { path: "$doc", preserveNullAndEmptyArrays: true } },
    {
        $addFields:{
            closedetail:"$closedetail.valuedescription",
            department:"$department.name",
           doc:"$doc.name",
           HN:"$HN.mrn",
           fullname:{
               $concat:["$HN.firstname"," ",  { "$ifNull": ["$HN.lastname", ""] }]
           }
        }
    },

])
