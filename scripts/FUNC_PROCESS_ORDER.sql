CREATE OR REPLACE FUNCTION process_order(email_address text, product_ids integer[]) RETURNS text AS
$$
DECLARE
    customer_record customer;
    product_records product[];
    total_cost numeric;
	order_id integer;
BEGIN
	-- Step 1: Validate User/Authorization
   	SELECT * INTO customer_record FROM customer WHERE email = email_address LIMIT 1 FOR UPDATE;
	
	-- Step 2: Check Inventory Availability
    SELECT ARRAY_AGG(p) INTO product_records
    FROM product p
    WHERE productid = ANY(product_ids) AND stock >= 1;
--     FOR UPDATE;

    -- Step 3: Calculate Order Total
    SELECT SUM(price) INTO total_cost
    FROM product
    WHERE productid = ANY(product_ids);
	
	-- Step 4: Generate Order Invoice
	
	INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
	VALUES
	(CURRENT_TIMESTAMP, customer_record.customerid, customer_record.customerid, total_cost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO order_id;

	
	-- Step 5: Insert Order Details
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	SELECT order_id, unnest(ARRAY[1, 2, 3]), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;
	
	-- Step 6: Process Payment (Assuming a successful payment)
    INSERT INTO payment (OrderID, CustomerID, Amount, PaymentDate, PaymentMethod, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
    VALUES (order_id, customer_record.customerid, total_cost, CURRENT_DATE, 'Credit Card', 'System', 'System', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

    -- Step 7: Update Inventory
    UPDATE product
    SET stock = stock - 1
    WHERE productid = ANY(product_ids);


    RETURN order_id;
EXCEPTION
    WHEN OTHERS THEN
        -- Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Thêm sản phẩm vào kho
CREATE OR REPLACE FUNCTION importProduct (name VARCHAR(100), description text, price DECIMAL(10,2), stock INT, categoryID INT, createdBy VARCHAR(50), updatedBy VARCHAR(50)) RETURNS INT AS
$$
BEGIN
    --STEP 1: Insert product into table
    INSERT INTO Product (ProductID, Name, Description, Price, Stock, CategoryID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
    VALUES
    ( name, description, price, stock, categoryID, createdBy, updatedBy, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning product;

    RETURN product.ProductID;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;


--Kiểm tra kho theo trong khoản thời gian từ A -> B
CREATE OR REPLACE FUNCTION checkWarehouseFromTo(timeFrom TIMESTAMP, timeTo TIMESTAMP) RETURNS Product[] AS
$$
DECLARE
        productList Product[];
BEGIN
   
    SELECT * 	INTO productList
    FROM product
    WHERE UpdatedAt BETWEEN timeFrom AND timeTo
    ORDER BY UpdatedAt;

	RETURN productList;
EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Xuất kho sản phẩm
CREATE OR REPLACE FUNCTION exportProduct( productIds INT[]) RETURNS INT[] AS
$$
DECLARE
    productIdList INT[];
BEGIN
    --Chọn các sản phẩm có trong list và kiểm tra xem sản phẩm còn không
    SELECT * INTO productIdList
    FROM Product
    WHERE Product.ProductID = ANY (product_ids) AND Product.stock > 0;

    --Cập nhật lại số lượng sản phẩm
    UPDATE Product
    SET stock = stock - 1
    WHERE ProductID = ANY(productIds);

    RETURN ProductID;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Xuất kho sản phẩm hết hạn sau 3 ngày
CREATE OR REPLACE FUNCTION exportProductExpired() RETURNS INT[] AS
$$
DECLARE
    productIdExpired INT[];
BEGIN
    --Lấy các sản phẩm đã quá hạn 3 ngày 
    SELECT * INTO productIdExpired
    FROM Product
    WHERE  Product.CreatedAt <=  NOW() - INTERVAL '3 DAYS' ; --Hàm interval để tính toán

    --Cập nhật lại số lượng sản phẩm thành 0
    UPDATE Product
    SET stock = 0
    WHERE ProductID = ANY(productIdExpired);

    RETURN productIdExpired;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Tìm sản phẩm theo tên và category name
CREATE OR REPLACE FUNCTION searchProductByNameProduct(nameProduct VARCHAR(100), nameCategory VARCHAR(50) ) RETURNS Product[] AS
$$
DECLARE
    productList Product[];
BEGIN
    SELECT * INTO productList
    FROM Product, Category
    WHERE  Product.Name ilike '%nameProduct%' AND Category.Name ilike '%nameCategory%' AND Product.CategoryId = Category.CategoryId;

    RETURN productList;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;
	


--Tổng sản phẩm bán ra trong khoản thời gian A -> B
CREATE OR REPLACE FUNCTION totalProductFromTo(timeFrom TIMESTAMP, timeTo TIMESTAMP ) RETURNS BIGINT AS  --BIGINT để tránh tràn số, Vd 1000 tỉ + 1000 tỉ => tràn số nếu là int
$$
DECLARE
	totalQuantity BIGINT;
BEGIN
    SELECT SUM (Quantity) AS Total 
	INTO totalQuantity
    FROM OrderDetails
    WHERE UpdatedAt BETWEEN timeFrom AND timeTo;  --UpdateAt là vì sẽ lấy lần cuối cùng cập nhật thông tin ( sửa đổi thông tin hoá đơn)


    RETURN totalQuantity;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Doanh thu trong khoản thời gian A -> B
CREATE OR REPLACE FUNCTION totalMountFromTo(timeFrom TIMESTAMP, timeTo TIMESTAMP ) RETURNS DECIMAL(12,2) AS
$$
DECLARE
	totalMount BIGINT;
BEGIN
    SELECT SUM (Ord.Quantity * Pro.Price) AS TotalMount
    INTO totalMount
    FROM OrderDetails  as Ord, Product as Pro
    WHERE Ord.UpdatedAt BETWEEN timeFrom AND timeTo AND Ord.ProductID=Pro.ProductID;
	
    RETURN totalMount;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Tổng Chi tiêu của 1 khách hàng tỏng khoản thời gian A -> B
CREATE OR REPLACE FUNCTION totalMountOfCustomerFromTo(timeFrom TIMESTAMP, timeTo TIMESTAMP, customerId INT ) RETURNS DECIMAL(12,2) AS
$$
DECLARE
	totalMountCustomer DECIMAL(12,2);
BEGIN
    SELECT SUM (Ord.Quantity * Pro.Price) 
    INTO totalMountCustomer
    FROM OrderDetails  as Ord, Product as Pro , Orders as Order1 , Customer as Cus 
    WHERE Ord.UpdatedAt BETWEEN timeFrom AND timeTo AND Ord.ProductID=Pro.ProductID AND Ord.OrderID=Order1.OrderID AND  Cus.CustomerID = Order1.CustomerID AND Cus.CustomerID=customerId ;


    RETURN totalMountCustomer;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Khuyến mãi, mua 2 tính tiền 1.
CREATE OR REPLACE FUNCTION discountBuyTwoPayOne(emailAddress text, productIdDiscount INT, quantityProductDiscount INT, listProductId INT[] ) RETURNS INT AS
$$
DECLARE
    customerRecord customer;
    productRecord product;
    product_dis_record product;
    totalCost DECIMAL(12,2);
    costProductDiscount DECIMAL(10,2);
	orderId INT;
BEGIN
    --Xác định thông tin khách hàng, 1 hoá đơn của 1 khách hàng
    SELECT * INTO customerRecord FROM customer WHERE email = emailAddress LIMIT 1 FOR UPDATE;
   
    -- Tính tiền tất cả sản phẩm trong danh sách sản phẩm mua trừ sản phẩm giảm giá
    SELECT SUM(price) INTO totalCost
    FROM product
    WHERE productid = ANY(listProductId);
    
    -- Tính tiền sản phẩm giảm giá
    SELECT ( quantityProductDiscount/2 * Product.Price) INTO costProductDiscount
    FROM Product
    WHERE Product.ProductID = productIdDiscount AND quantityProductDiscount >1;

    --Tổng tiền sau giảm giá
    totalCost := totalCost + costProductDiscount;

    --Thêm vào bảng order
    INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
	VALUES
	(CURRENT_TIMESTAMP, customerRecord.customerid, customerRecord.customerid, totalCost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO orderId;

    --Thêm vào bảng chi tiết hoá đơn list product
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	SELECT orderId, unnest(listProductId), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

     --Thêm vào bảng chi tiết hoá đơn product discount
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	VALUES
	(orderId, productIdDiscount, quantityProductDiscount, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
    
    --Cập nhật lại số lượng sản phẩm
    UPDATE Product
    SET stock = stock -1
    WHERE ProductID = ANY(listProductId);

    --Cập nhật lại số lượng sản phẩm khuyến mãi
    UPDATE Product
    SET stock = stock - quantityProductDiscount
    WHERE ProductID = productIdDiscount;

    RETURN orderId;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Khuyến mãi, đơn trên X tiền thì giảm Y phần trăm
CREATE OR REPLACE FUNCTION discountForBillOverMoney(emailAddress text, percentDiscount DECIMAL(12,2),listProductId INT[] ) RETURNS INT AS
$$
DECLARE
    customerRecord customer;
    totalCost DECIMAL(12,2);
	orderId INT;
BEGIN
    --Xác định thông tin khách hàng, 1 hoá đơn của 1 khách hàng
    SELECT * INTO customerRecord FROM customer WHERE email = emailAddress LIMIT 1 FOR UPDATE;
    -- Tính tiền tất cả sản phẩm trong danh sách sản phẩm mua 
    SELECT SUM(price) INTO totalCost
    FROM product
    WHERE productid = ANY(listProductId);
    
    --Tính lại đơn tiền sau khi áp dụng giảm giá
    totalCost:=totalCost*(100-percentDiscount);

    --Thêm vào bảng order
    INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
	VALUES
	(CURRENT_TIMESTAMP, customer_record.customerid, customer_record.customerid, total_cost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO orderId;

     --Thêm vào bảng chi tiết hoá đơn list product
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	SELECT orderId, unnest(listProductId), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

    --Cập nhật lại số lượng sản phẩm
    UPDATE Product
    SET stock = stock -1
    WHERE ProductID = ANY(listProductId);

    RETURN orderId;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Khuyến mãi, mua trên n sản phẩm cùng danh mục 
CREATE OR REPLACE FUNCTION discountForBillHasSameProductCategory(emailAddress text, percentDiscount DECIMAL(12,2),listProductId INT[], numberProductSameCategory INT, categoryIdDiscount INT ) RETURNS INT AS
$$
DECLARE
    customerRecord customer;
    totalCost DECIMAL(12,2);
	orderId INT;
    countProductSameCategory INT;
BEGIN
    --Xác định thông tin khách hàng, 1 hoá đơn của 1 khách hàng
    SELECT * INTO customerRecord FROM customer WHERE email = emailAddress LIMIT 1 FOR UPDATE;
    -- Tính tiền tất cả sản phẩm trong danh sách sản phẩm mua 
    SELECT SUM(price) INTO totalCost
    FROM product
    WHERE productid = ANY(listProductId);

    --Đếm các sản phầm cùng category
    SELECT Count (productid) INTO countProductSameCategory
    FROM product
    WHERE categoryid = categoryIdDiscount
    
    -- Xác định đủ điều kiện để nhận khuyến mãi và tính lại đơn tiền sau khi đủ điều kiện áp dụng giảm giá
    IF countProductSameCategory > numberProductSameCategory THEN
        totalCost:=totalCost*(100-percentDiscount);
    END IF;
    
    --Thêm vào bảng order
    INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
	VALUES
	(CURRENT_TIMESTAMP, customer_record.customerid, customer_record.customerid, total_cost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO orderId;

     --Thêm vào bảng chi tiết hoá đơn list product
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	SELECT orderId, unnest(listProductId), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

    --Cập nhật lại số lượng sản phẩm
    UPDATE Product
    SET stock = stock -1
    WHERE ProductID = ANY(listProductId);

    RETURN orderId;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Giảm giá theo phiếu mua hàng
CREATE OR REPLACE FUNCTION discountCoupon(emailAddress text, valueDiscount DECIMAL(12,2),listProductId INT[],) RETURNS INT AS
$$
DECLARE
    customerRecord customer;
    totalCost DECIMAL(12,2);
	orderId INT;
BEGIN
    --Xác định thông tin khách hàng, 1 hoá đơn của 1 khách hàng
    SELECT * INTO customerRecord FROM customer WHERE email = emailAddress LIMIT 1 FOR UPDATE;
    -- Tính tiền tất cả sản phẩm trong danh sách sản phẩm mua 
    SELECT SUM(price) INTO totalCost
    FROM product
    WHERE productid = ANY(listProductId);

    -- Áp dụng giảm giá phiếu mua hàng
    totalCost:=totalCost - valueDiscount;
    
    --Thêm vào bảng order
    INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
	VALUES
	(CURRENT_TIMESTAMP, customer_record.customerid, customer_record.customerid, total_cost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO orderId;

     --Thêm vào bảng chi tiết hoá đơn list product
    INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
	SELECT orderId, unnest(listProductId), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

    --Cập nhật lại số lượng sản phẩm
    UPDATE Product
    SET stock = stock -1
    WHERE ProductID = ANY(listProductId);

    RETURN orderId;

EXCEPTION
    WHEN OTHERS THEN    
        --Rollback the transaction on any exception
        RAISE NOTICE 'An error occurred. Rolling back transaction.';
        ROLLBACK;
        RAISE;
END;
$$
LANGUAGE plpgsql;

--Giảm giá khi mua theo danh mục
-- CREATE OR REPLACE FUNCTION discountCoupon(emailAddress text, valueDiscount DECIMAL(12,2),listProductId INT[],) RETURNS INT AS
-- $$
-- DECLARE
--     customerRecord customer;
--     totalCost DECIMAL(12,2);
-- 	orderId INT;
-- BEGIN
--     --Xác định thông tin khách hàng, 1 hoá đơn của 1 khách hàng
--     SELECT * INTO customerRecord FROM customer WHERE email = emailAddress LIMIT 1 FOR UPDATE;
--     -- Tính tiền tất cả sản phẩm trong danh sách sản phẩm mua 
--     SELECT SUM(price) INTO totalCost
--     FROM product
--     WHERE productid = ANY(listProductId);

--     -- Áp dụng giảm giá phiếu mua hàng
--     totalCost:=totalCost - valueDiscount;
    
--     --Thêm vào bảng order
--     INSERT INTO Orders (OrderDate, CustomerID, EmployeeID, TotalAmount, DeliveryAddressID, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
-- 	VALUES
-- 	(CURRENT_TIMESTAMP, customer_record.customerid, customer_record.customerid, total_cost, 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) returning orderid INTO orderId;

--      --Thêm vào bảng chi tiết hoá đơn list product
--     INSERT INTO orderdetails (orderid, productid, quantity, createdby, updatedby, createdat, updatedat)
-- 	SELECT orderId, unnest(listProductId), 1, 'Admin', 'Admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP;

--     --Cập nhật lại số lượng sản phẩm
--     UPDATE Product
--     SET stock = stock -1
--     WHERE ProductID = ANY(listProductId);

--     RETURN orderId;

-- EXCEPTION
--     WHEN OTHERS THEN    
--         --Rollback the transaction on any exception
--         RAISE NOTICE 'An error occurred. Rolling back transaction.';
--         ROLLBACK;
--         RAISE;
-- END;
-- $$
-- LANGUAGE plpgsql;