
val rawData = sc.textFile("/user/cloudera/rawdata/airline/flights")
val flightRDD = rawData
	.filter(!_.startsWith("Year"))
	.map(line => {
		val parts = line.split(',')

		parts(0) + "," + parts(1) + "," + parts(2) + "," + parts(3) + "," +
		(if (parts(4) == "NA") "" else parts(4)) + "," + 
		(if (parts(5) == "NA") "" else parts(5)) + "," + 
		(if (parts(6) == "NA") "" else parts(6)) + "," + 
		(if (parts(7) == "NA") "" else parts(7)) + "," + 
		(if (parts(8) == "NA") "" else parts(8)) + "," + 
		(if (parts(9) == "NA") "" else parts(9)) + "," + 
		(if (parts(10) == "NA") "" else parts(10)) + "," + 
		(if (parts(11) == "NA") "" else parts(11)) + "," + 
		(if (parts(12) == "NA") "" else parts(12)) + "," + 
		(if (parts(13) == "NA") "" else parts(13)) + "," + 
		(if (parts(14) == "NA") "" else parts(14)) + "," + 
		(if (parts(15) == "NA") "" else parts(15)) + "," + 
		(if (parts(16) == "NA") "" else parts(16)) + "," + 
		(if (parts(17) == "NA") "" else parts(17)) + "," + 
		(if (parts(18) == "NA") "" else parts(18)) + "," + 
		(if (parts(19) == "NA") "" else parts(19)) + "," + 
		(if (parts(20) == "NA") "" else parts(20)) + "," + 
		(if (parts(21) == "NA") "" else parts(21)) + "," + 
		(if (parts(22) == "NA") "" else parts(22)) + "," + 
		(if (parts(23) == "NA") "" else parts(23)) + "," + 
		(if (parts(24) == "NA") "" else parts(24)) + "," + 
		(if (parts(25) == "NA") "" else parts(25)) + "," + 
		(if (parts(26) == "NA") "" else parts(26)) + "," + 
		(if (parts(27) == "NA") "" else parts(27)) + "," + 
		(if (parts(28) == "NA") "" else parts(28)) + ","
	})

flight.saveAsTextFile("/user/cloudera/output/airline/flights")