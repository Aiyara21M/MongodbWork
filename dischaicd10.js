db.getCollection("patientbills").aggregate([
  {
    $match: {
      iscancelled: false,
      createdat: {
        $gte: ISODate("2024-08-26T00:00:00.000+0700"),
        $lte:  ISODate("2024-08-27T23:59:59.999+0700")
      }
    }
  },
      { $lookup: { from: 'patientvisits', localField: 'patientvisituid', foreignField: '_id', as: 'visit' } },
    { $unwind: { path: '$visit', preserveNullAndEmptyArrays: true } },
   { $lookup: { from: "referencevalues", localField: "visit.entypeuid", foreignField: "_id", as: "entypeuid" } },
 { $unwind: { path: "$entypeuid", preserveNullAndEmptyArrays: true } },
{
    $match:{
        "entypeuid.valuecode":"ENTYPE2"
    }
},
  {
      $group:{
          _id:{
              patientvisituid:"$patientvisituid"
          }
      }
  },
{
    $project:{
        _id:"$_id.patientvisituid"
    }
},
    {
        $lookup: {
            from: 'diagnoses',
            let: {
                visitid: "$_id",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$visitid', '$patientvisituid']
                        },
                    }
                },
  { $unwind: { path: '$codeddiagnosis', preserveNullAndEmptyArrays: true } },
  {
      $match:{
          "codeddiagnosis.isprimary" : true,
      }
  },
  {
      $project:{
          name:"$codeddiagnosis.problemcode",
          pbname:"$codeddiagnosis.problemname"
      }
  }
            ],
            as: 'Pdia'
        }
    },
  { $unwind: { path: '$Pdia', preserveNullAndEmptyArrays: true } },
  {
      $group:{
          _id:{
              "name":{$ifNull:["$Pdia.name",""]},
              "Pdia":{$ifNull:["$Pdia.pbname","No diagnosis"]},
          },
          diacount:{$sum:1}
      }
  },
  {
     $project:{
         _id:"$_id.name",
         name:"$_id.Pdia",
         diacount:"$diacount"
     } 
  },
{
    $sort:{
        diacount:-1
    }
}

])
