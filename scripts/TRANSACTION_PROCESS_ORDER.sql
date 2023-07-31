BEGIN TRANSACTION; 
	-- Step 1: Validate User/Authorization with email addresss
   	SELECT * INTO customer_record FROM customer WHERE email = 'johndoe@example.com' LIMIT 1 FOR UPDATE;
	
	-- Step 2: Check Inventory Availability with productid=1
    SELECT ARRAY_AGG(p) INTO product_records
    FROM product p
    WHERE productid = ANY(1) AND stock >= 1; 
	
	 -- Step 3: Calculate Order Total with product id=1
    SELECT SUM(price) INTO total_cost
    FROM product
    WHERE productid = ANY(1);
	
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
	
COMMIT; 


