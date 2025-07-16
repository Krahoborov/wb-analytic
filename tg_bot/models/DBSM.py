from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Integer, ForeignKey, Boolean, Column, Integer, DateTime, BigInteger, Float, Enum, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy import func
import enum
from datetime import datetime
engine = create_engine("sqlite:///db.sqlite3")


Base = declarative_base()

# Сначала определяем перечисление
class TaxSystemType(enum.Enum):
    USN_6 = "УСН 6%"
    NO_TAX = "Без налога"
    CUSTOM = "Произвольный процент"

class RegularExpenseFrequency(enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True)
    is_active = Column(Boolean, default=True)

    shops = relationship("Shop", back_populates="user")
    created_at = Column(DateTime, server_default=func.now())
    subscription_start = Column(DateTime, nullable=True)
    subscription_end = Column(DateTime, nullable=True)
    is_trial_used = Column(Boolean, default=False)
    last_active = Column(DateTime, default=datetime.utcnow)
    daily_reports_enabled = Column(Boolean, default=False)

class Shop(Base):
    __tablename__ = "shops"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=True)
    api_token = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, server_default=func.now())

    # Relationships
    tax_settings = relationship("TaxSystemSetting", back_populates="shop", uselist=False)
    product_costs = relationship("ProductCost", back_populates="shop")
    regular_expenses = relationship("RegularExpense", back_populates="shop")
    one_time_expenses = relationship("OneTimeExpense", back_populates="shop")
    
    user = relationship("User", back_populates="shops")

class CashedShopData(Base):
    __tablename__ = "cashed_shop_data"

    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer)
    cashed_all = Column(JSON)
    cashed_year = Column(JSON)
    cashed_month = Column(JSON)
    cashed_week = Column(JSON)


class Penalty(Base):
    __tablename__ = "penalties"

    id = Column(Integer, primary_key=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    nm_id = Column(Integer)
    sum = Column(Float)
    type = Column(String)
    date = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())


class Advertisement(Base):
    __tablename__ = "advertisements"

    id = Column(Integer, primary_key=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    amount = Column(Float)
    date = Column(DateTime)
    nmId = Column(BigInteger)
    created_at = Column(DateTime, server_default=func.now())
    advert_id = Column(Integer)


class Payment(Base):
    __tablename__ = 'payments'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer)
    amount = Column(Integer)
    months = Column(Integer)
    tarrif = Column(String(20), default='zero')  # zero/basic/extended
    payment_id = Column(String(255))
    status = Column(String(20), default='pending')  # pending/completed/failed
    created_at = Column(DateTime, default=func.now())


class Order(Base):
    __tablename__ = 'orders'
    
    srid = Column(String, primary_key=True)
    date = Column(DateTime)
    lastChangeDate = Column(DateTime)
    warehouseName = Column(String(50))
    warehouseType = Column(String)
    countryName = Column(String(200))
    oblastOkrugName = Column(String(200))
    regionName = Column(String(200))
    supplierArticle = Column(String(75))
    nmId = Column(Integer)
    barcode = Column(String(30))
    category = Column(String(50))
    subject = Column(String(50))
    brand = Column(String(50))
    techSize = Column(String(30))
    incomeID = Column(Integer)
    isSupply = Column(Boolean)
    isRealization = Column(Boolean)
    totalPrice = Column(Float)
    discountPercent = Column(Integer)
    spp = Column(Float)
    forPay = Column(Float)
    finishedPrice = Column(Float)
    priceWithDisc = Column(Float)
    isCancel = Column(Boolean)
    cancelDate = Column(DateTime)
    orderType = Column(String, nullable=True)
    sticker = Column(String)
    gNumber = Column(String(50))
    shop_id = Column(BigInteger)
    is_bouhght = Column(Boolean, default=False)
    cost_price = Column(Float, nullable=True)

# В tg_bot/models/DBSM.py
class TaxSystemSetting(Base):
    __tablename__ = 'tax_system_settings'
    
    id = Column(Integer, primary_key=True)
    shop_id = Column(Integer, ForeignKey('shops.id'), nullable=False)
    tax_system = Column(Enum(TaxSystemType), nullable=False)
    custom_percent = Column(Float, nullable=True)  # <-- Добавьте это поле
    created_at = Column(DateTime, server_default=func.now())
    
    shop = relationship("Shop", back_populates="tax_settings")

class ProductCost(Base):
    __tablename__ = "product_costs"
    
    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    article = Column(String(50), nullable=False)
    cost = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    
    shop = relationship("Shop", back_populates="product_costs")

class RegularExpense(Base):
    __tablename__ = "regular_expenses"
    
    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    amount = Column(Float, nullable=False)
    description = Column(Text, nullable=False)
    frequency = Column(Enum(RegularExpenseFrequency), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    shop = relationship("Shop", back_populates="regular_expenses")

class OneTimeExpense(Base):
    __tablename__ = "one_time_expenses"
    
    id = Column(Integer, primary_key=True, index=True)
    shop_id = Column(Integer, ForeignKey("shops.id"), nullable=False)
    amount = Column(Float, nullable=False)
    description = Column(Text, nullable=False)
    expense_date = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    shop = relationship("Shop", back_populates="one_time_expenses")

Base.metadata.create_all(bind=engine)