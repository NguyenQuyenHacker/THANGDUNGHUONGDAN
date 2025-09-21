import sqlite3
# Hàm thêm nhân sự
def them_nhan_su(ma_nv, ho_ten, sdt, ngay_sinh, gioi_tinh, dia_chi, chuc_vu, luong, ca_lam):
    conn = sqlite3.connect("nhahang.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Nhân_sự (Mã_nhân_viên, Họ_và_tên, Số_điện_thoại, Ngày_tháng_năm_sinh,
                             Giới_tính, Địa_chỉ, Chức_vụ, Lương, Ca_làm_việc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ma_nv, ho_ten, sdt, ngay_sinh, gioi_tinh, dia_chi, chuc_vu, luong, ca_lam))
    conn.commit()
    conn.close()

# Hàm thêm nguyên liệu
def them_nguyen_lieu(ma_nl, ten, nha_cung_cap, ngay_nhap, nsx_hsd, so_luong, don_vi):
    conn = sqlite3.connect("nhahang.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Nguyên_liệu (Mã_nguyên_liệu, Tên_nguyên_liệu, Nhà_cung_cấp, Ngày_nhập_kho,
                                NSX_HSD, Số_lượng, Đơn_vị_tính)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ma_nl, ten, nha_cung_cap, ngay_nhap, nsx_hsd, so_luong, don_vi))
    conn.commit()
    conn.close()

# Hàm thêm khách hàng
def them_khach_hang(ma_kh, ten, gioi_tinh, tuoi, lich_su, diem, phan_hoi):
    conn = sqlite3.connect("nhahang.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Khách_hàng (Mã_khách_hàng, Tên, Giới_tính, Tuổi, Lịch_sử_mua_hàng,
                               Điểm_thưởng, Phản_hồi)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ma_kh, ten, gioi_tinh, tuoi, lich_su, diem, phan_hoi))
    conn.commit()
    conn.close()

# Hàm thêm món ăn
def them_mon_an(ma_mon, ten, phan_loai, mo_ta, thanh_phan, thoi_gian, pho_bien, hinh_anh, gia):
    conn = sqlite3.connect("nhahang.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Món_ăn (Mã_món_ăn, Tên_món_ăn, Phân_loại, Mô_tả_hương_vị,
                           Thành_phần, Thời_gian_chế_biến, Độ_phổ_biến,
                           Hình_ảnh_minh_họa, Giá_thành)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ma_mon, ten, phan_loai, mo_ta, thanh_phan, thoi_gian, pho_bien, hinh_anh, gia))
    conn.commit()
    conn.close()

# Hàm thêm hóa đơn
def them_hoa_don(ma_hd, ma_kh, ma_nv, thoi_gian, giam_gia, tong):
    conn = sqlite3.connect("nhahang.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Hóa_đơn (Mã_hóa_đơn, Mã_khách_hàng, Mã_nhân_viên,
                             Thời_gian_xuất, giảm_giá, Tổng_giá_tiền)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (ma_hd, ma_kh, ma_nv, thoi_gian, giam_gia, tong))
    conn.commit()
    conn.close()
