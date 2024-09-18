db.getCollection("patientvisits").aggregate([


    {
        $match: {
            visitid: "67-2I008137"
        }
    },
        {
        $lookup: {
            from: 'allergies',
            let: {
                pid: "$patientuid",
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $eq: ['$$pid', '$patientuid']
                        },
                    }
                },
 { $unwind: { path: "$foodallergies", preserveNullAndEmptyArrays: true } },      
    { $lookup: { from: "orderresultitems", localField: "foodallergies.resultitemuid", foreignField: "_id", as: "foodallergie" } },
    { $unwind: { path: "$foodallergie", preserveNullAndEmptyArrays: true } },
{
    $addFields:{
        foodalg:{$concat:["$foodallergie.name"," : ",{$ifNull:["$foodallergies.comments",""]}]}
    }
},
{
    $project:{
        foodalert:"$foodalg"
    }
}
            ],
            as: 'foodalert'
        }
    },
 {
    $addFields: {
      foodalert: {
        $reduce: {
          input: "$foodalert",
          initialValue: "",
          in: {
            $concat: [
              "$$value",
              { $cond: [{ $eq: ["$$value", ""] }, "", ", " ] },
              "$$this.foodalert"
            ]
          }
        }
      }
    }
  },
    
  

    { $lookup: { from: "orderresultitems", localField: "allergies.foodallergies.resultitemuid", foreignField: "_id", as: "foodallergie" } },
    { $unwind: { path: "$foodallergie", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "patients", localField: "patientuid", foreignField: "_id", as: "HN" } },
    { $unwind: { path: "$HN", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "referencevalues", localField: "HN.titleuid", foreignField: "_id", as: "title" } },
    { $unwind: { path: "$title", preserveNullAndEmptyArrays: true } },
    {
        $addFields: {
            bed: { $last: "$bedoccupancy.beduid" },
            ward: { $last: "$bedoccupancy.warduid" }
        }
    },
    { $lookup: { from: "beds", localField: "bed", foreignField: "_id", as: "bed" } },
    { $unwind: { path: "$bed", preserveNullAndEmptyArrays: true } },
    { $lookup: { from: "wards", localField: "ward", foreignField: "_id", as: "ward" } },
    { $unwind: { path: "$ward", preserveNullAndEmptyArrays: true } },
    {
        $project: {
            visitid: 1,
            HN: "$HN.mrn",
            tile: "$title.valuedescription",
            fname: "$HN.firstname",
            lname: "$HN.lastname",
            HN: "$HN.mrn",
            foodallergie: "$foodalert",
            dateofbirth: "$HN.dateofbirth",
            isdobestimated: "$HN.isdobestimated",
            bed: "$bed.name",
            ward: "$ward.name"
        }
    }
    


])
