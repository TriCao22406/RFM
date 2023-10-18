import datetime as dt
import numpy as np
import pandas as pd
from datetime import datetime


def rfm(inputfile, outputfile, ngay_can_tinh):
    print(" ")
    print("---------------------------------------------")
    print(" tính toán RFM vào ngày " + ngay_can_tinh)
    print("---------------------------------------------")

    NOW = datetime.strptime(ngay_can_tinh, "%Y-%m-%d")

    #tạo dataframe don_hang
    don_hang = pd.read_csv(inputfile, sep=',', encoding="windows-1252")
    #chuyển dữ liệu chuỗi ngày đặt mua thành dữ liệu kiểu ngày tháng
    don_hang['ngay_dat_mua'] = pd.to_datetime(don_hang['ngay_dat_mua'])
    #nhóm các dữ liệu về khách hàng lại và tính toán
    bang_rfm = don_hang.groupby('khach_hang').agg({'ngay_dat_mua': lambda x: (NOW - x.max()).days,  # Recency
                                               'so_lan_mua_hang': lambda x: len(x),  # Frequency
                                               'tong_gia_tri_hang': lambda x: x.sum()})  # Monetary Value

    #bang_rfm['ngay_dat_mua'] = bang_rfm['ngay_dat_mua'].astype(int)
    #đổi tên để phù hợp với bảng rfm
    bang_rfm.rename(columns={'ngay_dat_mua': 'recency',
                             'so_lan_mua_hang': 'frequency',
                             'tong_gia_tri_hang': 'monetary_value'}, inplace=True)
    #tính toán ngũ phân vị
    ngu_phan_vi = bang_rfm.quantile(q=[0.2, 0.4, 0.6, 0.8])
    ngu_phan_vi = ngu_phan_vi.to_dict()

    phan_doan_khach_hang = bang_rfm
    #gán cột ngu_phan_vi cho df phan_doan_khach_hang và tính ngũ phân vị các mục bằng cách thực hiện hàm
    phan_doan_khach_hang['ngu_phan_vi_R'] = phan_doan_khach_hang['recency'].apply(ngu_phan_vi_recency, args=('recency', ngu_phan_vi,))
    phan_doan_khach_hang['ngu_phan_vi_F'] = phan_doan_khach_hang['frequency'].apply(ngu_phan_vi_frequency, args=('frequency', ngu_phan_vi,))
    phan_doan_khach_hang['ngu_phan_vi_M'] = phan_doan_khach_hang['monetary_value'].apply(ngu_phan_vi_monetary_value, args=('monetary_value', ngu_phan_vi,))

    #tổng kết về RFM khách hàng đó
    phan_doan_khach_hang['tong_ket_RFM'] = phan_doan_khach_hang.ngu_phan_vi_R.map(str) + \
                                           phan_doan_khach_hang.ngu_phan_vi_F.map(str) + \
                                           phan_doan_khach_hang.ngu_phan_vi_M.map(str)

    phan_doan_khach_hang.to_csv(outputfile, sep=',')

    print(" ")
    print(" Hoàn tất! Kiểm tra %s" % (outputfile))
    print(" ")

# x = giá trị phân loại ngũ phân vị, p = giá trị recency, d = dict ngũ phân vị
def ngu_phan_vi_recency(x, p, d):
    if x <= d[p][0.2]:
        return 5
    elif x <= d[p][0.4]:
        return 4
    elif x <= d[p][0.6]:
        return 3
    elif x <= d[p][0.8]:
        return 2
    else:
        return 1


# x = giá trị phân loại ngũ phân vị, p = giá trị frequency, d = dict ngũ phân vị
def ngu_phan_vi_frequency(x, p, d):
    if x <= d[p][0.2]:
        return 1
    elif x <= d[p][0.4]:
        return 2
    elif x <= d[p][0.6]:
        return 3
    elif x <= d[p][0.8]:
        return 4
    else:
        return 5

# x = giá trị phân loại ngũ phân vị, p = giá trị monetary_value, d = dict ngũ phân vị
def ngu_phan_vi_monetary_value(x, p, d):
    if x <= d[p][0.2]:
        return 1
    elif x <= d[p][0.4]:
        return 2
    elif x <= d[p][0.6]:
        return 3
    elif x <= d[p][0.8]:
        return 4
    else:
        return 5
rfm("sample-orders.csv","output.csv","2022-12-31")