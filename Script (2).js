db.getCollection("patientvisits").aggregate([
 {
     $match: {
          medicaldischargedate:{         
        $gte : ISODate("2024-04-23T00:00:00.000+07:00"),
        $lte : ISODate("2024-04-23T23:59:59.999+07:00") 
        },
    }
},


])