let
    UtmLatLong = (utmEast as nullable number,utmNorth as nullable number,utmZone as number) =>
        let

        if utmEast = null or utmNorth = null
        then
        in
        Table.FromRecords({[latitude = final_lat,longitude = final_long]})
        else
        diflat = -0.00066286966871111111111111111111111111,
        diflon = -0.0003868060578,

        zone = utmZone,
        c_sa = 6378137.000000,
        c_sb = 6356752.314245,
        e2 = Number.Power((Number.Power(c_sa,2) - Number.Power(c_sb,2)),0.5)/c_sb,
        e2cuadrada = Number.Power(e2,2),
        c = Number.Power(c_sa,2) / c_sb,
        x = utmEast - 500000,
        y = utmNorth,

        s = ((zone * 6.0) - 183.0),
        lat = y / (c_sa * 0.9996),
        v = (c / Number.Power(1 + (e2cuadrada * Number.Power(Number.Cos(lat), 2)), 0.5)) * 0.9996,
        a = x / v,
        a1 = Number.Sin(2 * lat),
        a2 = a1 * Number.Power((Number.Cos(lat)), 2),
        j2 = lat + (a1 / 2.0),
        j4 = ((3 * j2) + a2) / 4.0,
        j6 = ((5 * j4) + Number.Power(a2 * (Number.Cos(lat)), 2)) / 3.0,
        alfa = (3.0 / 4.0) * e2cuadrada,
        beta = (5.0 / 3.0) * Number.Power(alfa, 2),
        gama = (35.0 / 27.0) * Number.Power(alfa, 3),
        bm = 0.9996 * c * (lat - alfa * j2 + beta * j4 - gama * j6),
        b = (y - bm) / v,
        epsi = ((e2cuadrada * Number.Power(a, 2)) / 2.0) * Number.Power((Number.Cos(lat)), 2),
        eps = a * (1 - (epsi / 3.0)),
        nab = (b * (1 - epsi)) + lat,
        senoheps = (Number.Exp(eps) - Number.Exp(-eps)) / 2.0,
        delt  = Number.Atan(senoheps/(Number.Cos(nab) ) ),
        tao = Number.Atan(Number.Cos(delt) * Number.Tan(nab)),

        final_long = ((delt * (180.0 / Number.PI)) + s) + diflon,
        final_lat = ((lat + (1 + e2cuadrada * Number.Power(Number.Cos(lat), 2) - (3.0 / 2.0) * e2cuadrada * Number.Sin(lat) * Number.Cos(lat) * (tao - lat)) * (tao - lat)) * (180.0 / Number.PI)) + diflat

        in
        Table.FromRecords({[latitude = final_lat,longitude = final_long]})
in
UtmLatLong