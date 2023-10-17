import datetime as dt
import numpy as np
import pandas as pd
from datetime import datetime


def rfm(inputfile, outputfile, inputdate):
    print(" ")
    print("---------------------------------------------")
    print(" Calculating RFM segmentation for " + inputdate)
    print("---------------------------------------------")

    NOW = datetime.strptime(inputdate, "%Y-%m-%d")

    # Open orders file
    orders = pd.read_csv(inputfile, sep=',', encoding="windows-1252")
    orders['order_date'] = pd.to_datetime(orders['order_date'])

    rfmTable = orders.groupby('customer').agg({'order_date': lambda x: (NOW - x.max()).days,  # Recency
                                               'order_id': lambda x: len(x),  # Frequency
                                               'grand_total': lambda x: x.sum()})  # Monetary Value

    rfmTable['order_date'] = rfmTable['order_date'].astype(int)
    rfmTable.rename(columns={'order_date': 'recency',
                             'order_id': 'frequency',
                             'grand_total': 'monetary_value'}, inplace=True)

    quintiles = rfmTable.quantile(q=[0.20, 0.40, 0.60, 0.80])
    quintiles = quintiles.to_dict()

    rfmSegmentation = rfmTable

    rfmSegmentation['R_Quintile'] = rfmSegmentation['recency'].apply(RClass, args=('recency', quintiles,))
    rfmSegmentation['F_Quintile'] = rfmSegmentation['frequency'].apply(FMClass, args=('frequency', quintiles,))
    rfmSegmentation['M_Quintile'] = rfmSegmentation['monetary_value'].apply(FMClass,
                                                                            args=('monetary_value', quintiles,))

    rfmSegmentation['RFMScore'] = rfmSegmentation.R_Quartile.map(str) + rfmSegmentation.F_Quartile.map(
        str) + rfmSegmentation.M_Quartile.map(str)

    rfmSegmentation.to_csv(outputfile, sep=',')

    print(" ")
    print(" DONE! Check %s" % (outputfile))
    print(" ")


# We create two classes for the RFM segmentation since, being high recency is bad, while high frequency and monetary value is good.
# Arguments (x = value, p = recency, k = quartiles dict)
def RClass(x, p, d):
    if x <= d[p][0.20]:
        return 5
    elif x <= d[p][0.40]:
        return 4
    elif x <= d[p][0.60]:
        return 3
    elif x <= d[p][0.80]:
        return 2
    else:
        return 1


# Arguments (x = value, p = monetary_value, frequency, k = quartiles dict)
def FMClass(x, p, d):
    if x <= d[p][0.20]:
        return 1
    elif x <= d[p][0.40]:
        return 2
    elif x <= d[p][0.60]:
        return 3
    elif x <= d[p][0.80]:
        return 4
    else:
        return 5

rfm("sample-orders.csv","output.csv","2022-12-31")