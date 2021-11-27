USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_contact_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_contact_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel contact details.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_contact_details
    12123,
    '',
    '',
    'deva@agoda.com',
    '',
    '',
    '00000000-0000-0000-0000-000000000000',
    'GDPR',
    'Compliance',
    'compliance@gdpr.eu',
    'http://gdpr.eu/compliance',
    '0000000000',
    '00000',
    'Masked for GDPR compliance',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_contact_details(
    @hotel_id int,               -- Hotel ID to consider
    @first_name varchar(max),    -- First name to mask
    @last_name varchar(max),     -- Last name to mask
    @email_address varchar(max), -- Email address to mask
    @url varchar(max),           -- URL to mask
    @phone_number varchar(max),  -- Phone number to mask
    @rec_modified_by uniqueidentifier,
    @masked_first_name varchar(max),
    @masked_last_name varchar(max),
    @masked_email_address varchar(max),
    @masked_url varchar(max),
    @masked_phone_number varchar(max),
    @masked_postal_code varchar(max),
    @masked_other varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()
        DECLARE @full_name varchar = concat(@first_name, ' ', @last_name)
        DECLARE @masked_full_name varchar = concat(@masked_first_name, ' ', @masked_last_name)

        BEGIN TRANSACTION

        -- Agoda_Core.dbo.product_ec_hotel_contacts
IF OBJECT_ID('tempdb..#product_ec_hotel_contacts') IS NOT NULL DROP TABLE #product_ec_hotel_contacts

SELECT
pehc.product_ec_hotel_contact_id,
pehc.contact_name,
pehc.rec_modify_when,
pehc.rec_modify_by
INTO #product_ec_hotel_contacts
FROM Agoda_Core.dbo.product_ec_hotel_contacts AS pehc
INNER JOIN Agoda_Core.dbo.product_contacts AS pc ON pehc.product_email_contact_id = pc.product_contact_id
WHERE
pehc.hotel_id = @hotel_id
AND lower(ltrim(rtrim(pc.contact_method_value))) = @email_address

IF EXISTS(SELECT 1 FROM #product_ec_hotel_contacts)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_ec_hotel_contacts'
        SELECT * FROM #product_ec_hotel_contacts
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_name = @masked_full_name,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by
        FROM Agoda_Core.dbo.product_ec_hotel_contacts AS tbl
        INNER JOIN #product_ec_hotel_contacts AS stg ON tbl.product_ec_hotel_contact_id = stg.product_ec_hotel_contact_id
    END
END

DROP TABLE #product_ec_hotel_contacts

-- Agoda_Core.dbo.product_contacts
IF OBJECT_ID('tempdb..#product_contacts_email_address') IS NOT NULL DROP TABLE #product_contacts_email_address

SELECT
pc.product_contact_id,
pc.contact_method_value,
pc.contact_method_remark,
pc.rec_modify_when,
pc.rec_modify_by
INTO #product_contacts_email_address
FROM Agoda_Core.dbo.product_contacts AS pc

WHERE
product_id = @hotel_id
AND lower(ltrim(rtrim(contact_method_value))) = @email_address

IF EXISTS(SELECT 1 FROM #product_contacts_email_address)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_contacts'
        SELECT * FROM #product_contacts_email_address
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_method_value = @masked_email_address,
contact_method_remark = @masked_other,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by
        FROM Agoda_Core.dbo.product_contacts AS tbl
        INNER JOIN #product_contacts_email_address AS stg ON tbl.product_contact_id = stg.product_contact_id
    END
END

DROP TABLE #product_contacts_email_address

-- Agoda_Core.dbo.product_contacts
IF OBJECT_ID('tempdb..#product_contacts_phone_number') IS NOT NULL DROP TABLE #product_contacts_phone_number

SELECT
pc.product_contact_id,
pc.contact_method_value,
pc.contact_method_remark,
pc.rec_modify_when,
pc.rec_modify_by
INTO #product_contacts_phone_number
FROM Agoda_Core.dbo.product_contacts AS pc

WHERE
product_id = @hotel_id
AND ltrim(rtrim(contact_method_value)) = @phone_number

IF EXISTS(SELECT 1 FROM #product_contacts_phone_number)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_contacts'
        SELECT * FROM #product_contacts_phone_number
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_method_value = @masked_phone_number,
contact_method_remark = @masked_other,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by
        FROM Agoda_Core.dbo.product_contacts AS tbl
        INNER JOIN #product_contacts_phone_number AS stg ON tbl.product_contact_id = stg.product_contact_id
    END
END

DROP TABLE #product_contacts_phone_number

-- Agoda_Core.dbo.product_contacts
IF OBJECT_ID('tempdb..#product_contacts_url') IS NOT NULL DROP TABLE #product_contacts_url

SELECT
pc.product_contact_id,
pc.contact_method_value,
pc.contact_method_remark,
pc.rec_modify_when,
pc.rec_modify_by
INTO #product_contacts_url
FROM Agoda_Core.dbo.product_contacts AS pc

WHERE
product_id = @hotel_id
AND ltrim(rtrim(contact_method_value)) = @url

IF EXISTS(SELECT 1 FROM #product_contacts_url)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_contacts'
        SELECT * FROM #product_contacts_url
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_method_value = @masked_url,
contact_method_remark = @masked_other,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by
        FROM Agoda_Core.dbo.product_contacts AS tbl
        INNER JOIN #product_contacts_url AS stg ON tbl.product_contact_id = stg.product_contact_id
    END
END

DROP TABLE #product_contacts_url

-- Agoda_Core.dbo.property_contact_person
IF OBJECT_ID('tempdb..#property_contact_person') IS NOT NULL DROP TABLE #property_contact_person

SELECT
pcp.property_contact_person_id,
pcp.contact_person_role_id,
pcp.note,
pcp.lastupdated_when,
pcp.lastupdated_by
INTO #property_contact_person
FROM Agoda_Core.dbo.property_contact_person AS pcp
INNER JOIN Agoda_Core.dbo.contact_person_method_mapping AS cpmm ON pcp.contact_person_id = cpmm.contact_person_id
WHERE
pcp.property_id = @hotel_id
AND lower(ltrim(rtrim(cpmm.contact_method_value))) = @email_address

IF EXISTS(SELECT 1 FROM #property_contact_person)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.property_contact_person'
        SELECT * FROM #property_contact_person
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_person_role_id = NULL,
note = @masked_other,
lastupdated_when = @now,
lastupdated_by = @rec_modified_by
        FROM Agoda_Core.dbo.property_contact_person AS tbl
        INNER JOIN #property_contact_person AS stg ON tbl.property_contact_person_id = stg.property_contact_person_id
    END
END

DROP TABLE #property_contact_person

-- Agoda_Core.dbo.contact_person_method_mapping
IF OBJECT_ID('tempdb..#contact_person_method_mapping_email_address') IS NOT NULL DROP TABLE #contact_person_method_mapping_email_address

SELECT
cpmm.contact_person_id,
cpmm.contact_method_id,
cpmm.contact_method_value,
cpmm.is_active,
cpmm.lastupdated_by,
cpmm.lastupdated_when
INTO #contact_person_method_mapping_email_address
FROM Agoda_Core.dbo.contact_person_method_mapping AS cpmm
INNER JOIN Agoda_Core.dbo.property_contact_person AS pcp ON cpmm.contact_person_id = pcp.contact_person_id
WHERE
pcp.property_id = @hotel_id
AND lower(ltrim(rtrim(cpmm.contact_method_value))) = @email_address

IF EXISTS(SELECT 1 FROM #contact_person_method_mapping_email_address)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.contact_person_method_mapping'
        SELECT * FROM #contact_person_method_mapping_email_address
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_method_value = @masked_email_address,
is_active = 0,
lastupdated_by = @rec_modified_by,
lastupdated_when = @now
        FROM Agoda_Core.dbo.contact_person_method_mapping AS tbl
        INNER JOIN #contact_person_method_mapping_email_address AS stg ON tbl.contact_person_id = stg.contact_person_id
AND tbl.contact_method_id = stg.contact_method_id
AND tbl.contact_method_value = stg.contact_method_value
    END
END

DROP TABLE #contact_person_method_mapping_email_address

-- Agoda_Core.dbo.contact_person_method_mapping
IF OBJECT_ID('tempdb..#contact_person_method_mapping_phone_number') IS NOT NULL DROP TABLE #contact_person_method_mapping_phone_number

SELECT
cpmm.contact_person_id,
cpmm.contact_method_id,
cpmm.contact_method_value,
cpmm.is_active,
cpmm.lastupdated_by,
cpmm.lastupdated_when
INTO #contact_person_method_mapping_phone_number
FROM Agoda_Core.dbo.contact_person_method_mapping AS cpmm
INNER JOIN Agoda_Core.dbo.property_contact_person AS pcp ON cpmm.contact_person_id = pcp.contact_person_id
WHERE
pcp.property_id = @hotel_id
AND ltrim(rtrim(cpmm.contact_method_value)) = @phone_number

IF EXISTS(SELECT 1 FROM #contact_person_method_mapping_phone_number)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.contact_person_method_mapping'
        SELECT * FROM #contact_person_method_mapping_phone_number
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_method_value = @masked_phone_number,
is_active = 0,
lastupdated_by = @rec_modified_by,
lastupdated_when = @now
        FROM Agoda_Core.dbo.contact_person_method_mapping AS tbl
        INNER JOIN #contact_person_method_mapping_phone_number AS stg ON tbl.contact_person_id = stg.contact_person_id
AND tbl.contact_method_id = stg.contact_method_id
AND tbl.contact_method_value = stg.contact_method_value
    END
END

DROP TABLE #contact_person_method_mapping_phone_number

-- Agoda_Core.dbo.contact_person
IF OBJECT_ID('tempdb..#contact_person') IS NOT NULL DROP TABLE #contact_person

SELECT
cp.contact_person_id,
cp.contact_prefix_type_id,
cp.first_name,
cp.last_name,
cp.is_active,
cp.lastupdated_by,
cp.lastupdated_when
INTO #contact_person
FROM Agoda_Core.dbo.contact_person AS cp
INNER JOIN Agoda_Core.dbo.property_contact_person AS pcp ON cp.contact_person_id = pcp.contact_person_id
WHERE
pcp.property_id = @hotel_id
AND lower(ltrim(rtrim(cp.first_name))) = @first_name
AND lower(ltrim(rtrim(cp.last_name))) = @last_name

IF EXISTS(SELECT 1 FROM #contact_person)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.contact_person'
        SELECT * FROM #contact_person
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET contact_prefix_type_id = NULL,
first_name = @masked_first_name,
last_name = @masked_last_name,
is_active = 0,
lastupdated_by = @rec_modified_by,
lastupdated_when = @now
        FROM Agoda_Core.dbo.contact_person AS tbl
        INNER JOIN #contact_person AS stg ON tbl.contact_person_id = stg.contact_person_id
    END
END

DROP TABLE #contact_person

-- Agoda_Core.dbo.product_ec_hotel_contact_info
IF OBJECT_ID('tempdb..#product_ec_hotel_contact_info') IS NOT NULL DROP TABLE #product_ec_hotel_contact_info

SELECT
pehci.registration_id,
pehci.country_id,
pehci.state_id,
pehci.city_id,
pehci.street_address,
pehci.postal_code,
pehci.main_phone_number,
pehci.fax_number,
pehci.website_url,
pehci.main_contact_name,
pehci.contact_role_id,
pehci.contact_role_other,
pehci.email_address,
pehci.language_preference,
pehci.rec_modify_when,
pehci.rec_modify_by,
pehci.area_id,
pehci.latitude,
pehci.longitude,
pehci.county_id
INTO #product_ec_hotel_contact_info
FROM Agoda_Core.dbo.product_ec_hotel_contact_info AS pehci

WHERE
pehci.hotel_id = @hotel_id
AND lower(ltrim(rtrim(pehci.email_address))) = @email_address

IF EXISTS(SELECT 1 FROM #product_ec_hotel_contact_info)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_ec_hotel_contact_info'
        SELECT * FROM #product_ec_hotel_contact_info
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET country_id = 0,
state_id = 0,
city_id = 0,
street_address = @masked_other,
postal_code = @masked_postal_code,
main_phone_number = @masked_phone_number,
fax_number = @masked_email_address,
website_url = @masked_url,
main_contact_name = @masked_full_name,
contact_role_id = 0,
contact_role_other = '',
email_address = @masked_email_address,
language_preference = 0,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by,
area_id = NULL,
latitude = NULL,
longitude = NULL,
county_id = NULL
        FROM Agoda_Core.dbo.product_ec_hotel_contact_info AS tbl
        INNER JOIN #product_ec_hotel_contact_info AS stg ON tbl.registration_id = stg.registration_id
    END
END

DROP TABLE #product_ec_hotel_contact_info


        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_feedback_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_feedback_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel feedback.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_feedback_details
    12123,
    'it_ycs4@agoda.com',
    'GDPR',
    'Compliance',
    'compliance@gdpr.eu',
    '0000000000',
    'Masked for GDPR compliance',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_feedback_details(
    @hotel_id int,               -- Hotel ID to consider
    @email_address varchar(max), -- Email address to mask
    @masked_first_name varchar(max),
    @masked_last_name varchar(max),
    @masked_email_address varchar(max),
    @masked_phone_number varchar(max),
    @masked_other varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()
        DECLARE @masked_full_name varchar = concat(@masked_first_name, ' ', @masked_last_name)

        BEGIN TRANSACTION

        -- Agoda_YCS.dbo.ycs4_contact_us_information
IF OBJECT_ID('tempdb..#ycs4_contact_us_information') IS NOT NULL DROP TABLE #ycs4_contact_us_information

SELECT
cui.contact_us_information_id,
cui.customer_name,
cui.customer_email,
cui.customer_phone,
cui.contact_us_detail
INTO #ycs4_contact_us_information
FROM Agoda_YCS.dbo.ycs4_contact_us_information AS cui

WHERE
hotel_id = @hotel_id
AND lower(ltrim(rtrim(cui.customer_name))) = @email_address OR lower(ltrim(rtrim(cui.customer_email))) = @email_address


IF EXISTS(SELECT 1 FROM #ycs4_contact_us_information)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_YCS.dbo.ycs4_contact_us_information'
        SELECT * FROM #ycs4_contact_us_information
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET customer_name = @masked_full_name,
customer_email = @masked_email_address,
customer_phone = @masked_phone_number,
contact_us_detail = @masked_other
        FROM Agoda_YCS.dbo.ycs4_contact_us_information AS tbl
        INNER JOIN #ycs4_contact_us_information AS stg ON tbl.contact_us_information_id = stg.contact_us_information_id
    END
END

DROP TABLE #ycs4_contact_us_information

-- Agoda_YCS.dbo.ycs4_contact_us_register
IF OBJECT_ID('tempdb..#ycs4_contact_us_register') IS NOT NULL DROP TABLE #ycs4_contact_us_register

SELECT
cur.contact_us_id,
cur.customer_name,
cur.customer_email,
cur.country_id,
cur.contact_us_detail
INTO #ycs4_contact_us_register
FROM Agoda_YCS.dbo.ycs4_contact_us_register AS cur

WHERE
lower(ltrim(rtrim(cur.customer_name))) = @email_address OR lower(ltrim(rtrim(cur.customer_email))) = @email_address


IF EXISTS(SELECT 1 FROM #ycs4_contact_us_register)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_YCS.dbo.ycs4_contact_us_register'
        SELECT * FROM #ycs4_contact_us_register
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET customer_name = @masked_full_name,
customer_email = @masked_email_address,
country_id = -1,
contact_us_detail = @masked_other
        FROM Agoda_YCS.dbo.ycs4_contact_us_register AS tbl
        INNER JOIN #ycs4_contact_us_register AS stg ON tbl.contact_us_id = stg.contact_us_id
    END
END

DROP TABLE #ycs4_contact_us_register

-- Agoda_YCS.dbo.ycs4_user_feedback
IF OBJECT_ID('tempdb..#ycs4_user_feedback') IS NOT NULL DROP TABLE #ycs4_user_feedback

SELECT
uf.feedback_id,
uf.email,
uf.comment
INTO #ycs4_user_feedback
FROM Agoda_YCS.dbo.ycs4_user_feedback AS uf

WHERE
lower(ltrim(rtrim(uf.email))) = @email_address

IF EXISTS(SELECT 1 FROM #ycs4_user_feedback)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_YCS.dbo.ycs4_user_feedback'
        SELECT * FROM #ycs4_user_feedback
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET email = @masked_email_address,
comment = @masked_other
        FROM Agoda_YCS.dbo.ycs4_user_feedback AS tbl
        INNER JOIN #ycs4_user_feedback AS stg ON tbl.feedback_id = stg.feedback_id
    END
END

DROP TABLE #ycs4_user_feedback


        -- FOR TESTING PURPOSES; WILL BE CHANGED TO COMMIT WHEN OK.
        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_ec_contact_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_ec_contact_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel Express Connect details.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_ec_contact_details
    'seamdev01@agoda.com',
    '00000000-0000-0000-0000-000000000000',
    'GDPR',
    'Compliance',
    'compliance@gdpr.eu',
    'http://gdpr.eu/compliance',
    '0000000000',
    '00000',
    'Masked for GDPR compliance',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_ec_contact_details(
    @email_address varchar(max), -- Email address to mask
    @rec_modified_by uniqueidentifier,
    @masked_first_name varchar(max),
    @masked_last_name varchar(max),
    @masked_email_address varchar(max),
    @masked_url varchar(max),
    @masked_phone_number varchar(max),
    @masked_postal_code varchar(max),
    @masked_other varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()
        DECLARE @masked_full_name varchar = concat(@masked_first_name, ' ', @masked_last_name)

        BEGIN TRANSACTION

        -- Agoda_Core.dbo.product_ec_hotel_contact_info
IF OBJECT_ID('tempdb..#product_ec_hotel_contact_info') IS NOT NULL DROP TABLE #product_ec_hotel_contact_info

SELECT
pehci.registration_id,
pehci.country_id,
pehci.state_id,
pehci.city_id,
pehci.street_address,
pehci.postal_code,
pehci.main_phone_number,
pehci.fax_number,
pehci.website_url,
pehci.main_contact_name,
pehci.contact_role_id,
pehci.contact_role_other,
pehci.email_address,
pehci.language_preference,
pehci.rec_modify_when,
pehci.rec_modify_by,
pehci.area_id,
pehci.latitude,
pehci.longitude,
pehci.county_id
INTO #product_ec_hotel_contact_info
FROM Agoda_Core.dbo.product_ec_hotel_contact_info AS pehci

WHERE
pehci.hotel_id IS NULL
AND lower(ltrim(rtrim(pehci.email_address))) = @email_address

IF EXISTS(SELECT 1 FROM #product_ec_hotel_contact_info)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.product_ec_hotel_contact_info'
        SELECT * FROM #product_ec_hotel_contact_info
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET country_id = 0,
state_id = 0,
city_id = 0,
street_address = @masked_other,
postal_code = @masked_postal_code,
main_phone_number = @masked_phone_number,
fax_number = @masked_email_address,
website_url = @masked_url,
main_contact_name = @masked_full_name,
contact_role_id = 0,
contact_role_other = '',
email_address = @masked_email_address,
language_preference = 0,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by,
area_id = NULL,
latitude = NULL,
longitude = NULL,
county_id = NULL
        FROM Agoda_Core.dbo.product_ec_hotel_contact_info AS tbl
        INNER JOIN #product_ec_hotel_contact_info AS stg ON tbl.registration_id = stg.registration_id
    END
END

DROP TABLE #product_ec_hotel_contact_info


        -- FOR TESTING PURPOSES; WILL BE CHANGED TO COMMIT WHEN OK.
        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_security_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_security_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel Express Connect details.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_security_details
    '40F8A2D6-545E-48E0-B57D-0A764E15B7AC',
    '0000000000',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_security_details(
    @user_id uniqueidentifier,
    @masked_phone_number varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION

        -- Agoda_YCS.dbo.ycs41_property_2fa_registration
IF OBJECT_ID('tempdb..#ycs41_property_2fa_registration') IS NOT NULL DROP TABLE #ycs41_property_2fa_registration

SELECT
p2r.ycs41_property_2fa_registration_id,
p2r.phone_no
INTO #ycs41_property_2fa_registration
FROM Agoda_YCS.dbo.ycs41_property_2fa_registration AS p2r

WHERE
user_id = @user_id

IF EXISTS(SELECT 1 FROM #ycs41_property_2fa_registration)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_YCS.dbo.ycs41_property_2fa_registration'
        SELECT * FROM #ycs41_property_2fa_registration
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET phone_no = @masked_phone_number
        FROM Agoda_YCS.dbo.ycs41_property_2fa_registration AS tbl
        INNER JOIN #ycs41_property_2fa_registration AS stg ON tbl.ycs41_property_2fa_registration_id = stg.ycs41_property_2fa_registration_id
    END
END

DROP TABLE #ycs41_property_2fa_registration


        -- FOR TESTING PURPOSES; WILL BE CHANGED TO COMMIT WHEN OK.
        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_bank_account_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_bank_account_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel bank account details.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_bank_account_details
    1,
    '00000000-0000-0000-0000-000000000000',
    'GDPR',
    'Compliance',
    '0000000000',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_bank_account_details(
    @hotel_id int, -- Hotel ID to consider
    @rec_modified_by uniqueidentifier,
    @masked_first_name varchar(max),
    @masked_last_name varchar(max),
    @masked_bank_account_number varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()
        DECLARE @masked_full_name varchar = concat(@masked_first_name, ' ', @masked_last_name)

        BEGIN TRANSACTION

        -- Agoda_Core.dbo.ycsf_bank_account
IF OBJECT_ID('tempdb..#ycsf_bank_account') IS NOT NULL DROP TABLE #ycsf_bank_account

SELECT
ba.account_id,
ba.account_name,
ba.account_no,
ba.country_id,
ba.rec_modify_when,
ba.rec_modify_by
INTO #ycsf_bank_account
FROM Agoda_Core.dbo.ycsf_bank_account AS ba

WHERE
hotel_id = @hotel_id

IF EXISTS(SELECT 1 FROM #ycsf_bank_account)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.ycsf_bank_account'
        SELECT * FROM #ycsf_bank_account
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET account_name = @masked_full_name,
account_no = @masked_bank_account_number,
country_id = NULL,
rec_modify_when = @now,
rec_modify_by = @rec_modified_by
        FROM Agoda_Core.dbo.ycsf_bank_account AS tbl
        INNER JOIN #ycsf_bank_account AS stg ON tbl.account_id = stg.account_id
    END
END

DROP TABLE #ycsf_bank_account


        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_hotel_auth_hot_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_hotel_auth_hot_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in hotel auth hot details.
--
/*
EXEC dbo.ycs41_mask_pii_hotel_auth_hot_details
    '',
    '',
    '',
    'deva@agoda.com',
    'GDPR',
    'Compliance',
    'gdpr_compliance',
    'compliance@gdpr.eu',
    'Masked for GDPR compliance',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_hotel_auth_hot_details(
    @first_name varchar(max),
    @last_name varchar(max),
    @username varchar(max),
    @email_address varchar(max),
    @masked_first_name varchar(max),
    @masked_last_name varchar(max),
    @masked_username varchar(max),
    @masked_email_address varchar(max),
    @masked_other varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()
        DECLARE @full_name varchar = concat(@first_name, ' ', @last_name)
        DECLARE @masked_full_name varchar = concat(@masked_first_name, ' ', @masked_last_name)

        BEGIN TRANSACTION

        -- Agoda_Core.dbo.auth_ycs_hot_director
IF OBJECT_ID('tempdb..#auth_ycs_hot_director') IS NOT NULL DROP TABLE #auth_ycs_hot_director

SELECT
ahd.Country,
ahd.Name,
ahd.Position,
ahd.Username,
ahd.email_address
INTO #auth_ycs_hot_director
FROM Agoda_Core.dbo.auth_ycs_hot_director AS ahd

WHERE
lower(ltrim(rtrim(ahd.email_address))) = @email_address

IF EXISTS(SELECT 1 FROM #auth_ycs_hot_director)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.auth_ycs_hot_director'
        SELECT * FROM #auth_ycs_hot_director
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET Country = '',
Name = @masked_full_name,
Position = @masked_other,
Username = @masked_username,
email_address = @masked_email_address
        FROM Agoda_Core.dbo.auth_ycs_hot_director AS tbl
        INNER JOIN #auth_ycs_hot_director AS stg ON tbl.Country = stg.Country
AND tbl.Name = stg.Name
AND tbl.Position = stg.Position
AND tbl.Username = stg.Username
AND tbl.email_address = stg.email_address
    END
END

DROP TABLE #auth_ycs_hot_director

-- Agoda_Core.dbo.auth_ycs_hot_operations
IF OBJECT_ID('tempdb..#auth_ycs_hot_operations') IS NOT NULL DROP TABLE #auth_ycs_hot_operations

SELECT
aho.Country,
aho.Name,
aho.Position,
aho.Username,
aho.email_address
INTO #auth_ycs_hot_operations
FROM Agoda_Core.dbo.auth_ycs_hot_operations AS aho

WHERE
lower(ltrim(rtrim(aho.email_address))) = @email_address

IF EXISTS(SELECT 1 FROM #auth_ycs_hot_operations)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.auth_ycs_hot_operations'
        SELECT * FROM #auth_ycs_hot_operations
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET Country = '',
Name = @masked_full_name,
Position = @masked_other,
Username = @masked_username,
email_address = @masked_email_address
        FROM Agoda_Core.dbo.auth_ycs_hot_operations AS tbl
        INNER JOIN #auth_ycs_hot_operations AS stg ON tbl.Country = stg.Country
AND tbl.Name = stg.Name
AND tbl.Position = stg.Position
AND tbl.Username = stg.Username
AND tbl.email_address = stg.email_address
    END
END

DROP TABLE #auth_ycs_hot_operations


        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

USE Agoda_YCS
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ycs41_mask_pii_in_ycs3_details' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[ycs41_mask_pii_in_ycs3_details] AS SELECT 1 AS NUM')
END
GO

--
-- Masks known PII data found in YCS 3 details.
--
/*
EXEC dbo.ycs41_mask_pii_in_ycs3_details
    1,
    'deva',
    'deva@agoda.com',
    '00000000-0000-0000-0000-000000000000',
    'gdpr_compliance',
    'compliance@gdpr.eu',
    1
*/
ALTER PROCEDURE dbo.ycs41_mask_pii_in_ycs3_details(
    @hotel_id int, -- Hotel ID to consider
    @username varchar(max),
    @email_address varchar(max),
    @rec_modified_by uniqueidentifier,
    @masked_username varchar(max),
    @masked_email_address varchar(max),
    @dry_run bit -- Dry run: when set, prints the changes without actually applying them
)
AS
BEGIN
    BEGIN TRY
        DECLARE @now datetime = getdate()

        BEGIN TRANSACTION

        -- Agoda_Core.dbo.ycs30_booking_push
IF OBJECT_ID('tempdb..#ycs30_booking_push') IS NOT NULL DROP TABLE #ycs30_booking_push

SELECT
bp.hotel_id,
bp.client_site_username,
bp.client_site_password,
bp.rec_modified_by,
bp.rec_modified_when
INTO #ycs30_booking_push
FROM Agoda_Core.dbo.ycs30_booking_push AS bp

WHERE
hotel_id = @hotel_id
AND ltrim(rtrim(bp.client_site_username)) = @username

IF EXISTS(SELECT 1 FROM #ycs30_booking_push)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.ycs30_booking_push'
        SELECT * FROM #ycs30_booking_push
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET client_site_username = @masked_username,
client_site_password = NULL,
rec_modified_by = @rec_modified_by,
rec_modified_when = @now
        FROM Agoda_Core.dbo.ycs30_booking_push AS tbl
        INNER JOIN #ycs30_booking_push AS stg ON tbl.hotel_id = stg.hotel_id
    END
END

DROP TABLE #ycs30_booking_push

-- Agoda_Core.dbo.ycs31_channel_managers
IF OBJECT_ID('tempdb..#ycs31_channel_managers') IS NOT NULL DROP TABLE #ycs31_channel_managers

SELECT
cm.channel_manager_id,
cm.email,
cm.rec_modify_by,
cm.rec_modify_when
INTO #ycs31_channel_managers
FROM Agoda_Core.dbo.ycs31_channel_managers AS cm

WHERE
lower(ltrim(rtrim(cm.email))) = @email_address

IF EXISTS(SELECT 1 FROM #ycs31_channel_managers)
BEGIN
    IF @dry_run = 1
    BEGIN
        SELECT 'Found matches in Agoda_Core.dbo.ycs31_channel_managers'
        SELECT * FROM #ycs31_channel_managers
    END
    ELSE
    BEGIN
        UPDATE tbl
        SET email = @masked_email_address,
rec_modify_by = @rec_modified_by,
rec_modify_when = @now
        FROM Agoda_Core.dbo.ycs31_channel_managers AS tbl
        INNER JOIN #ycs31_channel_managers AS stg ON tbl.channel_manager_id = stg.channel_manager_id
    END
END

DROP TABLE #ycs31_channel_managers


        -- FOR TESTING PURPOSES; WILL BE CHANGED TO COMMIT WHEN OK.
        ROLLBACK
    END TRY

    BEGIN CATCH
        DECLARE @err_msg nvarchar(max) = ERROR_MESSAGE()

        IF xact_state() <> 0 ROLLBACK

        RAISERROR(@err_msg, 16, 1)
    END CATCH
END
GO

