package com.sparkScala.flatspec

import org.scalatest._


class ClsGeoSpatialfileIngestionSpec extends FlatSpec with Matchers {

  "A geomtery" should "be converted into JSON" in {
    val geometry = "MULTIPOLYGON (((-103.60887789697682 -75.107233366480457, -103.60887610585365 -75.107232354693451, -103.60887610585365 -75.107234262042041, -103.60887789697682 -75.107233366480457)), ((-103.60887789697682 -75.107233366480457, -103.61925208241622 -75.113093637042084, -103.52470862538503 -75.115412972979527, -103.81212317616627 -75.153010629229541, -103.93114173085377 -75.153865121417027, -104.10136878163492 -75.120784066729556, -104.33482825429117 -75.124934457354584, -104.55064856679127 -75.1607010589171, -104.63261878163497 -75.159968637042027, -104.76085364491628 -75.141291879229556, -104.78062903554122 -75.134822152667056, -104.79875647694747 -75.12481238704207, -104.81126868397865 -75.113826058917056, -104.81615149647877 -75.111140512042027, -104.83623206288497 -75.106745980792027, -105.0264176097599 -75.107844613604584, -105.06407630116624 -75.112483285479541, -105.09630286366622 -75.1240799651671, -105.10631262929122 -75.146540902667056, -105.10301673085377 -75.153376840167027, -105.09972083241621 -75.157038949542056, -105.09904944569755 -75.1607010589171, -105.10417639882253 -75.167048715167056, -105.12047278554127 -75.176326058917084, -105.14177405507255 -75.181330941729527, -105.40172278554124 -75.200862191729556, -105.66167151600992 -75.220393441729584, -105.67540442616617 -75.226863168292084, -105.70225989491622 -75.244929574542027, -105.71696936757249 -75.251643441729556, -106.08302771718186 -75.284236215167041, -106.44908606679122 -75.316828988604527, -106.45787512929124 -75.321589730792013, -106.48094641835372 -75.355281137042027, -106.52519690663497 -75.372859262042056, -106.59123694569752 -75.378718637042013, -106.65538489491624 -75.370417855792013, -106.71452796132242 -75.333552621417056, -106.81517493397878 -75.330500863604556, -106.90489661366627 -75.309993051104556, -107.11430823475992 -75.309016488604527, -107.1290787425724 -75.313655160479584, -107.15092932850992 -75.329524301104527, -107.16405188710372 -75.336726449542084, -107.19292151601003 -75.343074105792027, -107.29076087147878 -75.340998910479556, -107.31627356679124 -75.336848519854513, -107.36290442616622 -75.318660043292027, -107.38768469960371 -75.312922738604527, -107.86607825429115 -75.323542855792056, -108.02733313710374 -75.283747933917027, -108.0646866527288 -75.266536019854584, -108.10368811757255 -75.256526254229584, -108.20042884022874 -75.24993445735457, -108.18614661366617 -75.248713754229584, -108.16496741444742 -75.244319222979584, -108.10619055897872 -75.223811410479584, -108.08659827382252 -75.220637582354556, -107.98424231679115 -75.221370004229527, -107.91069495350997 -75.205989144854584, -107.76518714100992 -75.191828988604541, -107.7922257152288 -75.182551644854527, -108.04515540272872 -75.192561410479513, -108.01610266835367 -75.174372933917041, -107.83336341054124 -75.143977426104556, -107.79454505116624 -75.141780160479556, -107.75615393788503 -75.143855355792041, -107.7843521800724 -75.133479379229556, -107.79942786366617 -75.1301834807921, -107.83006751210378 -75.128352426104584, -107.83903968007253 -75.126033090167056, -107.85857093007252 -75.112849496417027, -107.88750159413496 -75.101008676104513, -107.92088782460372 -75.096370004229556, -108.12859046132246 -75.100154183917027, -108.27037512929128 -75.081843637042027, -108.23589026600997 -75.071345590167013, -108.11650550038499 -75.066340707354556, -108.13963782460378 -75.064021371417027, -108.18626868397878 -75.065486215167056, -108.20958411366624 -75.063288949542056, -108.1631973949163 -75.053523324542027, -107.84825598866617 -75.046077035479513, -107.83287512929122 -75.042536996417084, -107.82213294179127 -75.035701058917027, -107.81670081288502 -75.028010629229556, -107.80968176991621 -75.001521371417084, -107.8502091136663 -75.001277230792027, -107.86949622304122 -74.996394418292027, -107.88628089100997 -74.985774301104584, -107.84465491444753 -74.973201058917084, -107.6265152660099 -74.951472543292013, -107.56871497304124 -74.92949988704207, -107.54832923085367 -74.92559363704207, -107.13194739491617 -74.912898324542056, -107.11009680897874 -74.908625863604584, -107.0741471019474 -74.890925668292027, -107.00639807851005 -74.886897347979584, -106.99193274647874 -74.880183480792056, -106.93632971913499 -74.841487191729584, -106.91728675038502 -74.835871957354584, -106.8561905589788 -74.830134652667084, -106.77623450429117 -74.808406137042084, -106.62218176991622 -74.741755746417013, -106.53899085194753 -74.732356332354556, -106.54741370350997 -74.740168832354541, -106.55180823475999 -74.748835824542027, -106.55180823475999 -74.7578690276671, -106.54686438710365 -74.767146371417027, -106.55913245350996 -74.777888558917084, -106.44225012929117 -74.791072152667027, -106.27525794179115 -74.781184457354556, -106.25816809804122 -74.788142465167056, -106.20518958241627 -74.8311112151671, -106.16228186757253 -74.842585824542027, -106.12340247304122 -74.836848519854527, -106.04417884022865 -74.811579965167027, -105.7184952464787 -74.8004105315733, -105.39281165272874 -74.789241097979584, -105.31816565663502 -74.768733285479584, -105.30339514882252 -74.769221566729513, -105.26872718007247 -74.776667855792027, -105.25200354726003 -74.778132699542056, -105.25755774647871 -74.792781137042013, -105.27049719960378 -74.802058480792041, -105.30308997304117 -74.814387582354556, -105.26463782460374 -74.821101449542013, -105.19139563710374 -74.863948129229527, -105.15312659413496 -74.86846473079207, -105.16087805897874 -74.880549691729541, -105.17137610585365 -74.889338754229556, -105.18394934804122 -74.894099496417056, -105.19829260976002 -74.894587777667056, -105.16234290272877 -74.921443246417027, -104.98216712147865 -74.993830941729527, -104.55266272694752 -75.088801644854513, -104.16460120350996 -75.091670297198277, -103.77653968007242 -75.094538949542027, -103.6701554027288 -75.081721566729584, -103.55656897694755 -75.085383676104527, -103.57194983632247 -75.090876840167084, -103.62108313710365 -75.101130746417041, -103.60887789697682 -75.107233366480457)))"
    val keyMap = Map("MULTILINESTRING" -> "MultiLineString", "LINESTRING" -> "LineString", "POLYGON" -> "Polygon", "POINT" -> "Point", "MULTIPOLYGON" -> "MultiPolygon")
    val arrGeom = geometry.replace("(","[").replace(")", "]")
      .split("\\b [\\[]")
    val coordinatesList = arrGeom{1}.split(",")
    var updt_coordinatesList = ""
    coordinatesList.foreach(x => updt_coordinatesList = updt_coordinatesList + "[" + x.replaceAll("\\d\\s", ",") + "]")
    updt_coordinatesList = updt_coordinatesList.replaceAll("\\]\\[", "\\],\\[")
    val geojson = "{\"type\": \"" + keyMap(arrGeom{0}) + "\", \"coordinates\": ["  + updt_coordinatesList + "}"
                  .stripMargin
    geojson should be ("{\"type\": \"MultiPolygon\", \"coordinates\": [[[[-103.6088778969768,-75.107233366480457],[ -103.6088761058536,-75.107232354693451],[ -103.6088761058536,-75.107234262042041],[ -103.6088778969768,-75.107233366480457]]],[ [[-103.6088778969768,-75.107233366480457],[ -103.6192520824162,-75.113093637042084],[ -103.5247086253850,-75.115412972979527],[ -103.8121231761662,-75.153010629229541],[ -103.9311417308537,-75.153865121417027],[ -104.1013687816349,-75.120784066729556],[ -104.3348282542911,-75.124934457354584],[ -104.5506485667912,-75.1607010589171],[ -104.6326187816349,-75.159968637042027],[ -104.7608536449162,-75.141291879229556],[ -104.7806290355412,-75.134822152667056],[ -104.7987564769474,-75.12481238704207],[ -104.8112686839786,-75.113826058917056],[ -104.8161514964787,-75.111140512042027],[ -104.8362320628849,-75.106745980792027],[ -105.026417609759,-75.107844613604584],[ -105.0640763011662,-75.112483285479541],[ -105.0963028636662,-75.1240799651671],[ -105.1063126292912,-75.146540902667056],[ -105.1030167308537,-75.153376840167027],[ -105.0997208324162,-75.157038949542056],[ -105.0990494456975,-75.1607010589171],[ -105.1041763988225,-75.167048715167056],[ -105.1204727855412,-75.176326058917084],[ -105.1417740550725,-75.181330941729527],[ -105.4017227855412,-75.200862191729556],[ -105.6616715160099,-75.220393441729584],[ -105.6754044261661,-75.226863168292084],[ -105.7022598949162,-75.244929574542027],[ -105.7169693675724,-75.251643441729556],[ -106.0830277171818,-75.284236215167041],[ -106.4490860667912,-75.316828988604527],[ -106.4578751292912,-75.321589730792013],[ -106.4809464183537,-75.355281137042027],[ -106.5251969066349,-75.372859262042056],[ -106.5912369456975,-75.378718637042013],[ -106.6553848949162,-75.370417855792013],[ -106.7145279613224,-75.333552621417056],[ -106.8151749339787,-75.330500863604556],[ -106.9048966136662,-75.309993051104556],[ -107.1143082347599,-75.309016488604527],[ -107.129078742572,-75.313655160479584],[ -107.1509293285099,-75.329524301104527],[ -107.1640518871037,-75.336726449542084],[ -107.1929215160100,-75.343074105792027],[ -107.2907608714787,-75.340998910479556],[ -107.3162735667912,-75.336848519854513],[ -107.3629044261662,-75.318660043292027],[ -107.3876846996037,-75.312922738604527],[ -107.8660782542911,-75.323542855792056],[ -108.0273331371037,-75.283747933917027],[ -108.064686652728,-75.266536019854584],[ -108.1036881175725,-75.256526254229584],[ -108.2004288402287,-75.24993445735457],[ -108.1861466136661,-75.248713754229584],[ -108.1649674144474,-75.244319222979584],[ -108.1061905589787,-75.223811410479584],[ -108.0865982738225,-75.220637582354556],[ -107.9842423167911,-75.221370004229527],[ -107.9106949535099,-75.205989144854584],[ -107.7651871410099,-75.191828988604541],[ -107.792225715228,-75.182551644854527],[ -108.0451554027287,-75.192561410479513],[ -108.0161026683536,-75.174372933917041],[ -107.8333634105412,-75.143977426104556],[ -107.7945450511662,-75.141780160479556],[ -107.7561539378850,-75.143855355792041],[ -107.784352180072,-75.133479379229556],[ -107.7994278636661,-75.1301834807921],[ -107.8300675121037,-75.128352426104584],[ -107.8390396800725,-75.126033090167056],[ -107.8585709300725,-75.112849496417027],[ -107.8875015941349,-75.101008676104513],[ -107.9208878246037,-75.096370004229556],[ -108.1285904613224,-75.100154183917027],[ -108.2703751292912,-75.081843637042027],[ -108.2358902660099,-75.071345590167013],[ -108.1165055003849,-75.066340707354556],[ -108.1396378246037,-75.064021371417027],[ -108.1862686839787,-75.065486215167056],[ -108.2095841136662,-75.063288949542056],[ -108.163197394916,-75.053523324542027],[ -107.8482559886661,-75.046077035479513],[ -107.8328751292912,-75.042536996417084],[ -107.8221329417912,-75.035701058917027],[ -107.8167008128850,-75.028010629229556],[ -107.8096817699162,-75.001521371417084],[ -107.850209113666,-75.001277230792027],[ -107.8694962230412,-74.996394418292027],[ -107.8862808910099,-74.985774301104584],[ -107.8446549144475,-74.973201058917084],[ -107.626515266009,-74.951472543292013],[ -107.5687149730412,-74.92949988704207],[ -107.5483292308536,-74.92559363704207],[ -107.1319473949161,-74.912898324542056],[ -107.1100968089787,-74.908625863604584],[ -107.074147101947,-74.890925668292027],[ -107.0063980785100,-74.886897347979584],[ -106.9919327464787,-74.880183480792056],[ -106.9363297191349,-74.841487191729584],[ -106.9172867503850,-74.835871957354584],[ -106.856190558978,-74.830134652667084],[ -106.7762345042911,-74.808406137042084],[ -106.6221817699162,-74.741755746417013],[ -106.5389908519475,-74.732356332354556],[ -106.5474137035099,-74.740168832354541],[ -106.5518082347599,-74.748835824542027],[ -106.5518082347599,-74.7578690276671],[ -106.5468643871036,-74.767146371417027],[ -106.5591324535099,-74.777888558917084],[ -106.4422501292911,-74.791072152667027],[ -106.2752579417911,-74.781184457354556],[ -106.2581680980412,-74.788142465167056],[ -106.2051895824162,-74.8311112151671],[ -106.1622818675725,-74.842585824542027],[ -106.1234024730412,-74.836848519854527],[ -106.0441788402286,-74.811579965167027],[ -105.718495246478,-74.8004105315733],[ -105.3928116527287,-74.789241097979584],[ -105.3181656566350,-74.768733285479584],[ -105.3033951488225,-74.769221566729513],[ -105.2687271800724,-74.776667855792027],[ -105.2520035472600,-74.778132699542056],[ -105.2575577464787,-74.792781137042013],[ -105.2704971996037,-74.802058480792041],[ -105.3030899730411,-74.814387582354556],[ -105.2646378246037,-74.821101449542013],[ -105.1913956371037,-74.863948129229527],[ -105.1531265941349,-74.86846473079207],[ -105.1608780589787,-74.880549691729541],[ -105.1713761058536,-74.889338754229556],[ -105.1839493480412,-74.894099496417056],[ -105.1982926097600,-74.894587777667056],[ -105.1623429027287,-74.921443246417027],[ -104.9821671214786,-74.993830941729527],[ -104.5526627269475,-75.088801644854513],[ -104.1646012035099,-75.091670297198277],[ -103.7765396800724,-75.094538949542027],[ -103.670155402728,-75.081721566729584],[ -103.5565689769475,-75.085383676104527],[ -103.5719498363224,-75.090876840167084],[ -103.6210831371036,-75.101130746417041],[ -103.6088778969768,-75.107233366480457]]]]}")
  }

}