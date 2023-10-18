# Khai báo các thư viện
import pandas as pd
from datetime import datetime


def rfm(inputfile, outputfile, ngay_can_tinh):
    print(" ")
    print("---------------------------------------------")
    print(" tính toán RFM vào ngày " + ngay_can_tinh)
    print("---------------------------------------------")

# Chuyển đổi ngay_can_tinh thành đối tượng datetime với định dạng %Y-%m-%d
    NOW = datetime.strptime(ngay_can_tinh, "%Y-%m-%d")

    #tạo dataframe don_hang để đọc tệp csv inputfile với dấu phân cách , và mã hóa "windows-1252"
    don_hang = pd.read_csv(inputfile, sep=',', encoding="windows-1252")
    
    #chuyển đổi cột ngay_dat_mua thành dữ liệu kiểu ngày tháng
    don_hang['ngay_dat_mua'] = pd.to_datetime(don_hang['ngay_dat_mua'])
    
    # Tạo bang_rfm
    # don_hang.groupby('khach_hang'):  nhóm khung dữ liệu don_hang theo cột 'khach_hang'
    # agg() áp dụng các hàm sau cho mỗi nhóm dữ liệu:
        # lambda x: (NOW - x.max()).days tính số ngày kể từ lần mua hàng cuối cùng của khách hàng.
        # lambda x: len(x) tính số đơn hàng của khách hàng.
        # lambda x: x.sum() tính tổng giá trị đơn hàng của khách hàng
    bang_rfm = don_hang.groupby('khach_hang').agg({'ngay_dat_mua': lambda x: (NOW - x.max()).days,  # Recency
                                               'so_lan_mua_hang': lambda x: len(x),  # Frequency
                                               'tong_gia_tri_hang': lambda x: x.sum()})  # Monetary Value

    #bang_rfm['ngay_dat_mua'] = bang_rfm['ngay_dat_mua'].astype(int)
    
    #đổi tên các cột lần lượt thành recency, frequency và monetary_value
    bang_rfm.rename(columns={'ngay_dat_mua': 'recency',
                             'so_lan_mua_hang': 'frequency',
                             'tong_gia_tri_hang': 'monetary_value'}, inplace=True)
    
    #tính toán ngũ phân vị cho mỗi cột trong bang_rfm
    ngu_phan_vi = bang_rfm.quantile(q=[0.2, 0.4, 0.6, 0.8])

    # Chuyển đổi kết quả tính toán thành một dictionary
    ngu_phan_vi = ngu_phan_vi.to_dict()

    phan_doan_khach_hang = bang_rfm

    # Gán giá trị ngũ phân vị cho cột 'recency' dựa trên hàm ngu_phan_vi_recency() và dictionary chứa các ngũ phân vị của cột 'recency'
    phan_doan_khach_hang['ngu_phan_vi_R'] = phan_doan_khach_hang['recency'].apply(ngu_phan_vi_recency, args=('recency', ngu_phan_vi,))
    # Gán giá trị ngũ phân vị cho cột 'frequency' dựa trên hàm ngu_phan_vi_frequency() và dictionary chứa các ngũ phân vị của cột 'frequency'
    phan_doan_khach_hang['ngu_phan_vi_F'] = phan_doan_khach_hang['frequency'].apply(ngu_phan_vi_frequency, args=('frequency', ngu_phan_vi,))
    # Gán giá trị ngũ phân vị cho cột 'monetary_value' dựa trên hàm ngu_phan_vi_monetary_value() và dictionary chứa các ngũ phân vị của cột 'monetary_value'
    phan_doan_khach_hang['ngu_phan_vi_M'] = phan_doan_khach_hang['monetary_value'].apply(ngu_phan_vi_monetary_value, args=('monetary_value', ngu_phan_vi,))

    # Tạo ra cột 'tong_ket_RFM' để gán giá trị RFM cho mỗi khách hàng bằng cách ghép nối các giá trị ngũ phân vị của các cột recency, frequency, và monetary_value.
    phan_doan_khach_hang['tong_ket_RFM'] = phan_doan_khach_hang.ngu_phan_vi_R.map(str) + \
                                           phan_doan_khach_hang.ngu_phan_vi_F.map(str) + \
                                           phan_doan_khach_hang.ngu_phan_vi_M.map(str)
    
    # Ghi bảng phan_doan_khach_hang vào tệp CSV outputfile với dấu phân cách ,
    phan_doan_khach_hang.to_csv(outputfile, sep=',')

    print(" ")
    print(" Hoàn tất! Kiểm tra %s" % (outputfile))
    print(" ")

# gán giá trị ngũ phân vị cho cột recency
    # x là giá trị của cột recency
    # p là tên của cột recency
    # d là dictionary chứa các giá trị ngũ phân vị của cột recency
def ngu_phan_vi_recency(x, p, d):
    # kết quả trả về:
        # x <= giá trị phân vị 20% của cột recency, thì hàm sẽ trả về giá trị 5
        # x <= giá trị phân vị 40% của cột recency, thì hàm sẽ trả về giá trị 4
        # x <= giá trị phân vị 60% của cột recency, thì hàm sẽ trả về giá trị 3
        # x <= giá trị phân vị 80% của cột recency, thì hàm sẽ trả về giá trị 2
        # x > giá trị phân vị 80% của cột recency, thì hàm sẽ trả về giá trị 1.
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

# gán giá trị ngũ phân vị cho cột frequency
    # x là giá trị của cột frequency
    # p là tên của cột frequency
    # d là dictionary chứa các giá trị ngũ phân vị của cột frequency
def ngu_phan_vi_frequency(x, p, d):
    # kết quả trả về:
        # x <= giá trị phân vị 20% của cột frequency, thì hàm sẽ trả về giá trị 1
        # x <= giá trị phân vị 40% của cột frequency, thì hàm sẽ trả về giá trị 2
        # x <= giá trị phân vị 60% của cột frequency, thì hàm sẽ trả về giá trị 3
        # x <= giá trị phân vị 80% của cột frequency, thì hàm sẽ trả về giá trị 4
        # x > giá trị phân vị 80% của cột frequency, thì hàm sẽ trả về giá trị 5.
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

# gán giá trị ngũ phân vị cho cột monetary_value
    # x là giá trị của cột monetary_value
    # p là tên của cột monetary_value
    # d là dictionary chứa các giá trị ngũ phân vị của cột monetary_value
def ngu_phan_vi_monetary_value(x, p, d):
    # kết quả trả về:
        # x <= giá trị phân vị 20% của cột monetary_value, thì hàm sẽ trả về giá trị 1
        # x <= giá trị phân vị 40% của cột monetary_value, thì hàm sẽ trả về giá trị 2
        # x <= giá trị phân vị 60% của cột monetary_value, thì hàm sẽ trả về giá trị 3
        # x <= giá trị phân vị 80% của cột monetary_value, thì hàm sẽ trả về giá trị 4
        # x > giá trị phân vị 80% của cột monetary_value, thì hàm sẽ trả về giá trị 5
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
