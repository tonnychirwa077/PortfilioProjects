DROP TABLE IF EXISTS nashville
CREATE TABLE nashville
	(uniqueID bigint,
	parcelID varchar(255),
	landuse varchar(255),
	propertyaddress varchar(255),
	saledate timestamp,
	saleprice varchar(255),
	legalreference varchar(255),
	soldasvacant varchar(255),
	ownername varchar(255),
	owneraddress varchar(255),
	acreage numeric(5,2),
	taxdistrict varchar(255),
	landvalue bigint,
	buildingvalue bigint,
	totalvalue bigint,
	yearbuilt int,
	bedrooms int,
	fullbath int,
	halfbath int)
	
COPY nashville
FROM 'D:\Tutorials\Alex the Analyst\Data Cleaning\Nashville Housing Data for Data Cleaning.csv'
DELIMITER ','
CSV HEADER;

SELECT * FROM nashville


--Standardize the date format

SELECT saledate, CAST(saledate AS date) AS saledateconverted FROM nashville

ALTER TABLE nashville
ADD saledateconverted date

UPDATE nashville
SET saledateconverted = CAST(saledate AS date)


--Populate property address

SELECT * FROM nashville
WHERE propertyaddress IS NULL
ORDER BY parcelid

SELECT a.uniqueid, b.uniqueid, a.parcelid, b.parcelid, a.propertyaddress, b.propertyaddress
FROM nashville as a
JOIN nashville as b 
ON a.parcelid = b.parcelid AND a.uniqueid <> b.uniqueid
-- The parcelids are not unique in the rows, we only want to join rows with same parcelid but
--different uniqueids.
WHERE a.propertyaddress IS NULL 
ORDER BY a.uniqueid

-- 'COALESCE is an alternative to ISNULL in Microsoft SSMS'
SELECT a.uniqueid, b.uniqueid, a.parcelid, b.parcelid, a.propertyaddress, b.propertyaddress,
		COALESCE(a.propertyaddress,b.propertyaddress)
FROM nashville as a
JOIN nashville as b
ON a.parcelid = b.parcelid AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL
ORDER BY a.uniqueid


--Populating the null values in nashville table

UPDATE nashville AS a
SET propertyaddress = COALESCE(a.propertyaddress,b.propertyaddress)
FROM nashville
JOIN nashville as b
ON nashville.parcelid = b.parcelid AND nashville.uniqueid <> b.uniqueid
WHERE nashville.propertyaddress IS NULL


--Breaking address into individual columns (Address, City, State)

SELECT *
FROM nashville

SELECT SUBSTRING(propertyaddress, 1, STRPOS(propertyaddress, ',')-1) AS propertysplitaddress,
		SUBSTRING(propertyaddress, STRPOS(propertyaddress, ',')+1, LENGTH(propertyaddress)) AS propertysplitcity
FROM nashville

ALTER TABLE nashville
ADD propertysplitaddress varchar(255), ADD propertysplitcity varchar(255)

UPDATE nashville
SET propertysplitaddress = SUBSTRING(propertyaddress, 1, STRPOS(propertyaddress, ',')-1)

UPDATE nashville
SET propertysplitcity = SUBSTRING(propertyaddress, STRPOS(propertyaddress, ',')+1, LENGTH(propertyaddress))

SELECT owneraddress FROM nashville
ORDER BY owneraddress ASC

SELECT SPLIT_PART(owneraddress, ',',1),
		SPLIT_PART(owneraddress, ',',2),
		SPLIT_PART(owneraddress, ',',3)
FROM nashville
ORDER BY owneraddress ASC

ALTER TABLE nashville
ADD ownersplitaddress varchar(255), ADD ownersplitcity varchar(255), ADD ownersplitstate varchar(255)

UPDATE nashville
SET ownersplitaddress = SPLIT_PART(owneraddress, ',',1)

UPDATE nashville
SET ownersplitcity = SPLIT_PART(owneraddress, ',',2)

UPDATE nashville
SET ownersplitstate = SPLIT_PART(owneraddress, ',',3)


--Change Y and N to Yes and No in "Sold as Vacant" field

SELECT soldasvacant, count (*) FROM nashville
GROUP BY 1
ORDER BY 2

SELECT soldasvacant, 
	CASE WHEN soldasvacant = 'Y' THEN 'YES'
		 WHEN soldasvacant = 'N' THEN 'NO'
		 ELSE soldasvacant
	END
FROM nashville
ORDER BY LENGTH(soldasvacant) ASC

UPDATE nashville
SET soldasvacant = CASE WHEN soldasvacant = 'Y' THEN 'YES'
		 WHEN soldasvacant = 'N' THEN 'NO'
		 ELSE soldasvacant
	END


--Remove duplicates


SELECT uniqueid FROM nashville
WHERE uniqueid IN (SELECT uniqueid FROM (SELECT uniqueid, ROW_NUMBER() OVER(
						PARTITION BY parcelid, propertyaddress, saleprice, saledate, legalreference
						ORDER BY uniqueid) AS row_num
FROM nashville
) as a
WHERE a.row_num > 1
)

DELETE FROM nashville
WHERE uniqueid IN (SELECT uniqueid FROM (SELECT uniqueid, ROW_NUMBER() OVER(
						PARTITION BY parcelid, propertyaddress, saleprice, saledate, legalreference
						ORDER BY uniqueid) AS row_num
FROM nashville
) as a
WHERE a.row_num > 1
)
RETURNING uniqueid


-- Deleting unused columns

SELECT * FROM nashville

ALTER TABLE nashville
DROP COLUMN saledate,
DROP COLUMN propertyaddress,
DROP COLUMN owneraddress,
DROP COLUMN taxdistrict
