db.getCollection("patientvisits").aggregate([
{
    $addFields:{
        admissdate:{$first:"$bedoccupancy.startdate"}
    }
},
{
    $match:{
        admissdate:{
       $gte: ISODate("2024-08-25T00:00:00.000+0700"),
        $lte:  ISODate("2024-08-27T23:59:59.999+0700")
        }
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
