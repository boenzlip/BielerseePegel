

import requests
import csv
import collections


URL_FLUSS = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%s_AbflussAFFRA.csv"
URL_FLUSS_B = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%s_AbflussPneumatik.csv"
URL_FLUSS_M = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%s_AbflussPneumatikoben.csv"

URL_PEGEL = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%s_PegelRadar.csv"
URL_PEGEL_Z = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%i_PegelPneumatik.csv"
URL_PEGEL_M = "https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_%s_PegelPneumatikoben.csv"

data_rivers = {
    'zihlkanal' : [2446, URL_PEGEL_Z, URL_FLUSS],
    'hagneck' : [2085, URL_PEGEL, URL_FLUSS],
    'bruegg' : [2029, URL_PEGEL_Z, URL_FLUSS_B],
    'murgenthal': [2063, URL_PEGEL_M, URL_FLUSS_M]
}
data_lakes = {
    'biel' : [2208, URL_PEGEL],
    'thun' : [2093, URL_PEGEL_Z],
    'neuchatel' : [2642, URL_PEGEL]
}


#https://www.hydrodaten.admin.ch/lhg/az/dwh/csv/BAFU_2085_AbflussAFFRA.csv
# hagneck# hagneck - deterministic: https://www.hydrodaten.admin.ch/graphs/2085/deterministic_forecasts_2085.json


header = [
    'time',
    'zihlkanal_flow',
    'zihlkanal_pegel',
    'hagneck_flow',
    'hagneck_pegel',
    'bruegg_flow',
    'bruegg_pegel',
    'biel_pegel',
    'thun_pegel',
    'murgenthal_flow',
    'murgenthal_pegel',
    'neuchatel_pegel'
]
data = {

}


def read_hydro_csv(csvContent, suffix):
    print("downloaded %s with %i lines - now processing" % (hydroName, len(csvContent)))
    csvContent = csvContent.replace('\0', '')
    csv_file = csv.reader(csvContent.split('\n'), delimiter=',')
    next(csv_file)
    for line in csv_file:
        t = line[0]
        if t not in data: data[t] = [0] * len(header)
        data[t][header.index(hydroName + suffix)] = line[1]



for river in data_rivers:
    hydroName = river
    hydroEntity = data_rivers[river]

    read_hydro_csv(requests.get(hydroEntity[2] % hydroEntity[0]).text, "_flow")
    read_hydro_csv(requests.get(hydroEntity[1] % hydroEntity[0]).text, "_pegel")

for lake in data_lakes:
    hydroName = lake
    hydroEntity = data_lakes[lake]

    read_hydro_csv(requests.get(hydroEntity[1] % hydroEntity[0]).text, "_pegel")


orderedData = collections.OrderedDict(sorted(data.items()))
#print (data)
#print(orderedData.keys())

csvOut = []
for key in orderedData:

    line = orderedData[key]
    line[0] = key

    if 0 not in line: csvOut.append(line)



with open("out.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(csvOut)


#for row in reader:
#    print(row)



