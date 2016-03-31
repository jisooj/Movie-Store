-- Jisoo Jung

-- ORDER matters: Customer cannot reference Plans if Plans hasn't been
-- created beforehand. Should create in this order: E2->E1->E3

-- E2 [examples: "Basic", "Rental Plus", "Super Access"]
CREATE TABLE Plans (
	pid int,
	name VARCHAR(30),
	maxRental int, 
	monthlyFee float,
	PRIMARY KEY(pid)
);

-- E1 
-- R1 = Each customer has exactly one plan 
CREATE TABLE Customer (
	cid int,
	pid int,
	login VARCHAR(30),
	password VARCHAR(30),
	firstName VARCHAR(30),
	lastName VARCHAR(30),
	PRIMARY KEY(cid),
	FOREIGN KEY(pid) REFERENCES Plans(pid)
);

-- E3
-- R2 = Each rental refers to exactly one customer. It also
-- refers to a single movie, but that's in a different database,
-- so we don't model that as a relationship.
CREATE TABLE Rental (
	cid int,
	mid int,
	status VARCHAR(6), -- rented->open, returned->closed
	checkOutDate DATETIME, -- YYYY-MM-DD hh:mm:ss[.nnn]
	FOREIGN KEY(cid) REFERENCES Customer(cid),
	CHECK (status = 'open' OR status = 'closed')
);
CREATE CLUSTERED INDEX rid on Rental(cid);

-- Insert tuples into tables
-- Order matters here too. Make Plans first.
INSERT INTO Plans VALUES(1, 'Basic', 2, 3);
INSERT INTO Plans VALUES(2, 'Rental Plus', 4, 5);
INSERT INTO Plans VALUES(3, 'Super Access', 6, 7);
INSERT INTO Plans VALUES(4, 'Premium', 8, 9);

INSERT INTO Customer VALUES(1, 1, 'cust1', 'pass1', 'yoyo', 'ma');
INSERT INTO Customer VALUES(2, 3, 'cust2', 'pass2', 'movie', 'lover');

--yyyy-mm-dd hh:mm:ss
INSERT INTO Rental VALUES(1, 1234, 'open', '2000-01-03 12:34:11');
INSERT INTO Rental VALUES(1, 5678, 'open', '2000-01-05 12:34:11');
INSERT INTO Rental VALUES(1, 3333, 'closed', '1998-04-11 20:03:11');

INSERT INTO Rental VALUES(2, 2222, 'open', '2000-01-03 12:34:11');
INSERT INTO Rental VALUES(2, 12001, 'closed', '2005-01-03 12:34:11');
INSERT INTO Rental VALUES(2, 12002, 'closed', '2005-01-03 12:34:21');
INSERT INTO Rental VALUES(2, 12003, 'closed', '2005-01-03 12:34:31');
INSERT INTO Rental VALUES(2, 12004, 'closed', '2005-01-03 12:34:41');