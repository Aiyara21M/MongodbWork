db.getCollection("patientvisits").aggregate([
 {
     $match: {
         startdate:{
	      $gte: ISODate("2024-04-24T00:00:00.000+07:00"),
          $lte: ISODate("2024-04-24T23:59:59.999+07:00")
        },
    }
},
{ $lookup: { from: "patients",localField: "patientuid",foreignField: "_id",as: "HN"}},
{ $unwind: { path: "$HN",preserveNullAndEmptyArrays: true}},
{ $lookup: { from: "diagnoses",localField: "_id",foreignField: "patientvisituid",as: "diagnoses"}},
{ $unwind: { path: "$diagnoses",preserveNullAndEmptyArrays: true}},
{ $lookup: { from: "departments",localField: "diagnoses.departmentuid",foreignField: "_id",as: "Department"}},
{ $unwind: { path: "$Department",preserveNullAndEmptyArrays: true}},
{ $lookup: { from: "referencevalues",localField: "entypeuid",foreignField: "_id",as: "type"}},
{ $unwind: { path: "$type",preserveNullAndEmptyArrays: true}},
{ $lookup: { from: "referencevalues",localField: "visitclosereason",foreignField: "_id",as: "status"}},
{ $unwind: { path: "$status",preserveNullAndEmptyArrays: true}},
{ $unwind: { path: "$diagnoses.codeddiagnosis",preserveNullAndEmptyArrays: true}},
{$match:{
"type.relatedvalue":"OPD"
}
},
{ $lookup: { from: "departments",localField: "diagnoses.departmentuid",foreignField: "_id",as: "depart"}},
{ $unwind: { path: "$depart",preserveNullAndEmptyArrays: true}},

{
  $match: {
    $or: [
      {"depart.code": "PREER" },
      {"depart.code": "ER" },
      {"Department.code":"ER"},
      {"Department.code": "PREER" },
    ]
  }
},
{
  $match: {
    "diagnoses.codeddiagnosis.isprimary": true
  }
},
{
    $project:{
        _id:1,     
        VN:"$visitid",
        date:"$createdat",
        HN:"$HN.mrn",
        fname:"$HN.firstname",
        lname:"$HN.lastname",
        dateofbirth:"$HN.dateofbirth",
        isdobestimated:"$HN.isdobestimated",
        department:"$depart.name",
        code:"$diagnoses.codeddiagnosis.problemcode",
        codename:{
      $switch: {
        branches: [
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R1048"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R1049"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R101"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R102"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R103"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R104"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "R119"] }, then: "Abdominal Pain" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N739"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N800"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N801"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N809"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N831"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N832"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N838"] }, then: "นรีเวชกรรม" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N300"] }, then: "Cystitis" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N309"] }, then: "Cystitis" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N938"] }, then: "Vaginal Bleeding" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N390"] }, then: "UTI" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N328"] }, then: "Disorder of vagina" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N898"] }, then: "Disorder of vagina" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N907"] }, then: "ประจำเดือนผิดปกติ" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N911"] }, then: "ประจำเดือนผิดปกติ" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N912"] }, then: "ประจำเดือนผิดปกติ" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N920"] }, then: "ประจำเดือนผิดปกติ" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N921"] }, then: "ประจำเดือนผิดปกติ" },
          { case: { $eq: ["$diagnoses.codeddiagnosis.problemcode", "N926"] }, then: "ประจำเดือนผิดปกติ" },
        ],
        default: "$diagnoses.codeddiagnosis.problemname"
      }
    }
    }
},
{
  $match: {
    $or: [
      {"code": "R1048" },
      {"code": "R1049" },
      {"code": "R101" },
      {"code": "R102" },
      {"code": "R103" },
      {"code": "R104" },
      {"code": "R119" },
      {"code": "N739" },
      {"code": "N800" },
      {"code": "N801" },
      {"code": "N809" },
      {"code": "N831" },
      {"code": "N832" },
      {"code": "N838" },
      {"code": "N300" },
      {"code": "N309" },
      {"code": "N938" },
      {"code": "N390" },
      {"code": "N328" },
      {"code": "N898" },
      {"code": "N907" },
      {"code": "N911" },
      {"code": "N912" },
      {"code": "N920" },
      {"code": "N921" },
      {"code": "N926" },
    ]
  }
},



])