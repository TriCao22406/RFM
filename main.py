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
    don_hang['ngay_dat_mua'] = pd.to_datetime(don_hang['ngay_dat_mua'], format='%m/%d/%y')

    # Tạo bang_rfm
    # don_hang.groupby('khach_hang'):  nhóm khung dữ liệu don_hang theo cột 'khach_hang'
    # agg() áp dụng các hàm sau cho mỗi nhóm dữ liệu:
        # lambda x: (NOW - x.max()).days tính số n`gày kể từ lần mua hàng cuối cùng của khách hàng.
        # lambda x: len(x) tính số đơn hàng của khách hàng.
        # lambda x: x.sum() tính tổng giá trị đơn hàng của khách hàng
    bang_rfm = don_hang.groupby('khach_hang').agg({'ngay_dat_mua': lambda x: (NOW - x.max()).days,  # Recency
                                               'ma_don_hang': lambda x: len(x),  # Frequency
                                               'tong_gia_tri_hang': lambda x: x.sum()})  # Monetary Value
    print(NOW - datetime.strptime(2019-1-1, "%Y-%m-%d"))
    #bang_rfm['ngay_dat_mua'] = bang_rfm['ngay_dat_mua'].astype(int)
    
    #đổi tên các cột lần lượt thành recency, frequency và monetary_value
    bang_rfm.rename(columns={'ngay_dat_mua': 'recency',
                             'ma_don_hang': 'frequency',
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

# rfm("sample-orders.csv","output.csv","2022-12-31")
def segment(value):
	if value == '555' or value == '554'  or value == '544' or value == '545' or value == '454' or value == '455' or value == '445':
		return 'Champions'
	elif value == '543' or  value == '444' or value == '435' or value == '355' or value == '354' or value == '345' or value == '344' or value == '335':
		return 'Loyal'
	elif value == '553' or  value == '551' or value == '552' or value == '541' or value == '542' or value == '533' or value == '532' or value == '531' or value == '452' or  value == '451' or value == '442' or value == '441' or value == '431' or value == '453' or value == '433' or value == '432' or value == '423' or  value == '353' or value == '352' or value == '351' or value == '342' or value == '341' or value == '333' or value == '323':
		return 'Potential Loyalist'
	elif value == '512' or  value == '511' or value == '422' or value == '421' or value == '412' or value == '411' or value == '311':
		return 'New Customers'
	elif value == '525' or  value == '524' or value == '523' or value == '522' or value == '521' or value == '515' or value == '514' or value == '513' or value == '425' or  value == '424' or value == '413' or value == '414' or value == '415' or value == '315' or value == '314' or value == '313':
		return 'Promising'
	elif value == '535' or  value == '534' or value == '443' or value == '434' or value == '343' or value == '334' or value == '325' or value == '324':
		return 'Need Attention'
	elif value == '331' or  value == '321' or value == '312' or value == '221' or value == '213' or value == '231' or value == '241' or value == '251':
		return 'About To Sleep'
	elif value == '255' or  value == '254' or value == '245' or value == '244' or value == '253' or value == '252' or value == '243' or value == '242' or value == '235' or  value == '234' or value == '225' or value == '224' or value == '153' or value == '152' or value == '145' or value == '143' or value == '142' or  value == '135' or value == '134' or value == '133' or value == '125' or value == '124':
		return "At Risk"
	elif value == '155' or  value == '154' or value == '144' or value == '214' or value == '215' or value == '115' or value == '114' or value == '113':
		return 'Cannot Lose Them'
	elif value == '332' or  value == '322' or value == '233' or value == '232' or value == '223' or value == '222' or value == '132' or value == '123' or value == '122' or value == '212' or value == '211':
		return 'Hibernating Customers'
	else:
		return 'Lost Customers'

df = pd.read_csv('output.csv')
df['Phan_cum_thang_5'] = df['Diem_RFM'].apply(str).apply(segment)

df.to_csv('output.csv', index=False)
