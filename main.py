import datetime as dt
import numpy as np
import pandas as pd
from datetime import datetime


def rfm(inputfile, outputfile, ngay_can_tinh):
    print(" ")
    print("---------------------------------------------")
    print(" tính toán RFM vào ngày " + ngay_can_tinh)
    print("---------------------------------------------")

    # Chuyển đổi ngay_can_tinh thành đối tượng datetime với định dạng %Y-%m-%d
    NOW = datetime.strptime(ngay_can_tinh, "%Y-%m-%d")

    #tạo dataframe don_hang
    don_hang = pd.read_csv(inputfile, sep=',', encoding="windows-1252")
    #chuyển dữ liệu chuỗi ngày đặt mua thành dữ liệu kiểu ngày tháng
    don_hang['ngay_dat_mua'] = pd.to_datetime(don_hang['ngay_dat_mua'])
    #nhóm các dữ liệu về khách hàng lại và tính toán
    # Nhóm khung dữ liệu don_hang theo cột 'khach_hang'
    # agg() áp dụng các hàm sau cho mỗi nhóm dữ liệu:
        # lambda x: (NOW - x.max()).days tính số ngày kể từ lần mua hàng cuối cùng của khách hàng.
        # lambda x: len(x) tính số lần mua hàng của khách hàng.
        # lambda x: x.sum() tính tổng giá trị đơn hàng của khách hàng.
    bang_rfm = don_hang.groupby('khach_hang').agg({'ngay_dat_mua': lambda x: (NOW - x.max()).days,  # Recency
                                               'so_lan_mua_hang': lambda x: len(x),  # Frequency
                                               'tong_gia_tri_hang': lambda x: x.sum()})  # Monetary Value
    # Chuyển đổi dữ liệu trong cột ngay_dat_mua của bảng RFM thành kiểu dữ liệu int
    #bang_rfm['ngay_dat_mua'] = bang_rfm['ngay_dat_mua'].astype(int)
    # đổi tên các cột trong bảng RFM lần lượt thành recency, frequency và monetary_value
    bang_rfm.rename(columns={'ngay_dat_mua': 'recency',
                             'so_lan_mua_hang': 'frequency',
                             'tong_gia_tri_hang': 'monetary_value'}, inplace=True)
    #tính toán tứ phân vị cho mỗi cột trong bảng RFM
    tu_phan_vi = bang_rfm.quantile(q=[0.25, 0.5, 0.75])
    # Chuyển đổi kết quả tính toán thành một dictionary
    tu_phan_vi = tu_phan_vi.to_dict()

    phan_doan_khach_hang = bang_rfm
    #gán cột tu_phan_vi cho df phan_doan_khach_hang và tính tứ phân vị các mục bằng cách thực hiện hàm
    phan_doan_khach_hang['tu_phan_vi_R'] = phan_doan_khach_hang['recency'].apply(tu_phan_vi_recency, args=('recency', tu_phan_vi,))
    phan_doan_khach_hang['tu_phan_vi_F'] = phan_doan_khach_hang['frequency'].apply(tu_phan_vi_frequency, args=('frequency', tu_phan_vi,))
    phan_doan_khach_hang['tu_phan_vi_M'] = phan_doan_khach_hang['monetary_value'].apply(tu_phan_vi_monetary_value, args=('monetary_value', tu_phan_vi,))

    #tổng kết về RFM khách hàng đó
    phan_doan_khach_hang['tong_ket_RFM'] = phan_doan_khach_hang.tu_phan_vi_R.map(str) + \
                                           phan_doan_khach_hang.tu_phan_vi_F.map(str) + \
                                           phan_doan_khach_hang.tu_phan_vi_M.map(str)

    phan_doan_khach_hang.to_csv(outputfile, sep=',')

    print(" ")
    print(" Hoàn tất! Kiểm tra %s" % (outputfile))
    print(" ")

# x = giá trị phân loại tứ phân vị, p = giá trị recency, d = dict tứ phân vị
def tu_phan_vi_recency(x, p, d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]:
        return 2
    else:
        return 1


# x = giá trị phân loại tứ phân vị, p = giá trị frequency, d = dict tứ phân vị
def tu_phan_vi_frequency(x, p, d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4

# x = giá trị phân loại tứ phân vị, p = giá trị monetary_value, d = dict tứ phân vị
def tu_phan_vi_monetary_value(x, p, d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4

rfm("sample-orders.csv","output.csv","2023-10-17")
