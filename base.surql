DEFINE NS grooveguessr;
USE NS grooveguessr;

DEFINE DB development;
USE DB development;

-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- ACCESSES
-- ------------------------------

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNUP (CREATE user CONTENT { email: $email, name: $name, password: crypto::argon2::generate($password) }) SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(password, $password)) WITH JWT ALGORITHM HS512 KEY 'QGpvHe9dP3ztkn2IcXJhKtsJPYtkpg99tTnz2L9fhYaQeWyHzLpDoMf13Iwgjf7KGxZxIGm6YsxuESGdKIMjAV0J4Gfej7YGtsnCh1xA7iuuHeEp36gTxRAV3zOVK7CL' WITH ISSUER KEY 'QGpvHe9dP3ztkn2IcXJhKtsJPYtkpg99tTnz2L9fhYaQeWyHzLpDoMf13Iwgjf7KGxZxIGm6YsxuESGdKIMjAV0J4Gfej7YGtsnCh1xA7iuuHeEp36gTxRAV3zOVK7CL' DURATION FOR TOKEN 1h, FOR SESSION NONE;

-- ------------------------------
-- FUNCTIONS
-- ------------------------------

DEFINE FUNCTION fn::get_age($birth: datetime, $death: datetime) -> int { RETURN IF $birth = d'0000-01-01T00:00:00Z' { -1 } ELSE IF $death = d'0000-01-01T00:00:00Z' { duration::years(time::now() - $birth) } ELSE { duration::years($death - $birth) }; } PERMISSIONS FULL;

-- ------------------------------
-- ANALYZERS
-- ------------------------------

DEFINE ANALYZER full_text TOKENIZERS BLANK FILTERS LOWERCASE,ASCII,SNOWBALL(ENGLISH);

-- ------------------------------
-- TABLE: acted
-- ------------------------------

DEFINE TABLE acted TYPE RELATION IN person OUT movie | show SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD character ON acted TYPE string PERMISSIONS FULL;
DEFINE FIELD in ON acted TYPE record<person> PERMISSIONS FULL;
DEFINE FIELD order ON acted TYPE int PERMISSIONS FULL;
DEFINE FIELD out ON acted TYPE record<movie | `show`> PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: acted
-- ------------------------------


-- ------------------------------
-- TABLE: collection
-- ------------------------------

DEFINE TABLE collection TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD backdrop ON collection TYPE string ASSERT string::is::url($value) PERMISSIONS FULL;
DEFINE FIELD id ON collection TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON collection TYPE string PERMISSIONS FULL;
DEFINE FIELD overview ON collection TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD poster ON collection TYPE string ASSERT string::is::url($value) PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: collection
-- ------------------------------


-- ------------------------------
-- TABLE: company
-- ------------------------------

DEFINE TABLE company TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD country ON company TYPE record<country> PERMISSIONS FULL;
DEFINE FIELD description ON company TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD id ON company TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON company TYPE string PERMISSIONS FULL;
DEFINE FIELD parent ON company TYPE record<company> | null PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: company
-- ------------------------------


-- ------------------------------
-- TABLE: country
-- ------------------------------

DEFINE TABLE country TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD english_name ON country TYPE string PERMISSIONS FULL;
DEFINE FIELD id ON country TYPE string PERMISSIONS FULL;
DEFINE FIELD iso_3166_1 ON country TYPE string ASSERT string::is::alpha($value) PERMISSIONS FULL;
DEFINE FIELD native_name ON country TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: country
-- ------------------------------

INSERT [ { english_name: 'Andorra', id: country:ad, iso_3166_1: 'AD', native_name: 'Andorra' }, { english_name: 'United Arab Emirates', id: country:ae, iso_3166_1: 'AE', native_name: 'United Arab Emirates' }, { english_name: 'Afghanistan', id: country:af, iso_3166_1: 'AF', native_name: 'Afghanistan' }, { english_name: 'Antigua and Barbuda', id: country:ag, iso_3166_1: 'AG', native_name: 'Antigua & Barbuda' }, { english_name: 'Anguilla', id: country:ai, iso_3166_1: 'AI', native_name: 'Anguilla' }, { english_name: 'Albania', id: country:al, iso_3166_1: 'AL', native_name: 'Albania' }, { english_name: 'Armenia', id: country:am, iso_3166_1: 'AM', native_name: 'Armenia' }, { english_name: 'Netherlands Antilles', id: country:an, iso_3166_1: 'AN', native_name: 'Netherlands Antilles' }, { english_name: 'Angola', id: country:ao, iso_3166_1: 'AO', native_name: 'Angola' }, { english_name: 'Antarctica', id: country:aq, iso_3166_1: 'AQ', native_name: 'Antarctica' }, { english_name: 'Argentina', id: country:ar, iso_3166_1: 'AR', native_name: 'Argentina' }, { english_name: 'American Samoa', id: country:as, iso_3166_1: 'AS', native_name: 'American Samoa' }, { english_name: 'Austria', id: country:at, iso_3166_1: 'AT', native_name: 'Austria' }, { english_name: 'Australia', id: country:au, iso_3166_1: 'AU', native_name: 'Australia' }, { english_name: 'Aruba', id: country:aw, iso_3166_1: 'AW', native_name: 'Aruba' }, { english_name: 'Azerbaijan', id: country:az, iso_3166_1: 'AZ', native_name: 'Azerbaijan' }, { english_name: 'Bosnia and Herzegovina', id: country:ba, iso_3166_1: 'BA', native_name: 'Bosnia & Herzegovina' }, { english_name: 'Barbados', id: country:bb, iso_3166_1: 'BB', native_name: 'Barbados' }, { english_name: 'Bangladesh', id: country:bd, iso_3166_1: 'BD', native_name: 'Bangladesh' }, { english_name: 'Belgium', id: country:be, iso_3166_1: 'BE', native_name: 'Belgium' }, { english_name: 'Burkina Faso', id: country:bf, iso_3166_1: 'BF', native_name: 'Burkina Faso' }, { english_name: 'Bulgaria', id: country:bg, iso_3166_1: 'BG', native_name: 'Bulgaria' }, { english_name: 'Bahrain', id: country:bh, iso_3166_1: 'BH', native_name: 'Bahrain' }, { english_name: 'Burundi', id: country:bi, iso_3166_1: 'BI', native_name: 'Burundi' }, { english_name: 'Benin', id: country:bj, iso_3166_1: 'BJ', native_name: 'Benin' }, { english_name: 'Bermuda', id: country:bm, iso_3166_1: 'BM', native_name: 'Bermuda' }, { english_name: 'Brunei Darussalam', id: country:bn, iso_3166_1: 'BN', native_name: 'Brunei' }, { english_name: 'Bolivia', id: country:bo, iso_3166_1: 'BO', native_name: 'Bolivia' }, { english_name: 'Brazil', id: country:br, iso_3166_1: 'BR', native_name: 'Brazil' }, { english_name: 'Bahamas', id: country:bs, iso_3166_1: 'BS', native_name: 'Bahamas' }, { english_name: 'Bhutan', id: country:bt, iso_3166_1: 'BT', native_name: 'Bhutan' }, { english_name: 'Burma', id: country:bu, iso_3166_1: 'BU', native_name: 'Burma' }, { english_name: 'Bouvet Island', id: country:bv, iso_3166_1: 'BV', native_name: 'Bouvet Island' }, { english_name: 'Botswana', id: country:bw, iso_3166_1: 'BW', native_name: 'Botswana' }, { english_name: 'Belarus', id: country:by, iso_3166_1: 'BY', native_name: 'Belarus' }, { english_name: 'Belize', id: country:bz, iso_3166_1: 'BZ', native_name: 'Belize' }, { english_name: 'Canada', id: country:ca, iso_3166_1: 'CA', native_name: 'Canada' }, { english_name: 'Cocos  Islands', id: country:cc, iso_3166_1: 'CC', native_name: 'Cocos (Keeling) Islands' }, { english_name: 'Congo', id: country:cd, iso_3166_1: 'CD', native_name: 'Democratic Republic of the Congo (Kinshasa)' }, { english_name: 'Central African Republic', id: country:cf, iso_3166_1: 'CF', native_name: 'Central African Republic' }, { english_name: 'Congo', id: country:cg, iso_3166_1: 'CG', native_name: 'Republic of the Congo (Brazzaville)' }, { english_name: 'Switzerland', id: country:ch, iso_3166_1: 'CH', native_name: 'Switzerland' }, { english_name: "Cote D'Ivoire", id: country:ci, iso_3166_1: 'CI', native_name: 'Côte d’Ivoire' }, { english_name: 'Cook Islands', id: country:ck, iso_3166_1: 'CK', native_name: 'Cook Islands' }, { english_name: 'Chile', id: country:cl, iso_3166_1: 'CL', native_name: 'Chile' }, { english_name: 'Cameroon', id: country:cm, iso_3166_1: 'CM', native_name: 'Cameroon' }, { english_name: 'China', id: country:cn, iso_3166_1: 'CN', native_name: 'China' }, { english_name: 'Colombia', id: country:co, iso_3166_1: 'CO', native_name: 'Colombia' }, { english_name: 'Costa Rica', id: country:cr, iso_3166_1: 'CR', native_name: 'Costa Rica' }, { english_name: 'Serbia and Montenegro', id: country:cs, iso_3166_1: 'CS', native_name: 'Serbia and Montenegro' }, { english_name: 'Cuba', id: country:cu, iso_3166_1: 'CU', native_name: 'Cuba' }, { english_name: 'Cape Verde', id: country:cv, iso_3166_1: 'CV', native_name: 'Cape Verde' }, { english_name: 'Christmas Island', id: country:cx, iso_3166_1: 'CX', native_name: 'Christmas Island' }, { english_name: 'Cyprus', id: country:cy, iso_3166_1: 'CY', native_name: 'Cyprus' }, { english_name: 'Czech Republic', id: country:cz, iso_3166_1: 'CZ', native_name: 'Czech Republic' }, { english_name: 'Germany', id: country:de, iso_3166_1: 'DE', native_name: 'Germany' }, { english_name: 'Djibouti', id: country:dj, iso_3166_1: 'DJ', native_name: 'Djibouti' }, { english_name: 'Denmark', id: country:dk, iso_3166_1: 'DK', native_name: 'Denmark' }, { english_name: 'Dominica', id: country:dm, iso_3166_1: 'DM', native_name: 'Dominica' }, { english_name: 'Dominican Republic', id: country:do, iso_3166_1: 'DO', native_name: 'Dominican Republic' }, { english_name: 'Algeria', id: country:dz, iso_3166_1: 'DZ', native_name: 'Algeria' }, { english_name: 'Ecuador', id: country:ec, iso_3166_1: 'EC', native_name: 'Ecuador' }, { english_name: 'Estonia', id: country:ee, iso_3166_1: 'EE', native_name: 'Estonia' }, { english_name: 'Egypt', id: country:eg, iso_3166_1: 'EG', native_name: 'Egypt' }, { english_name: 'Western Sahara', id: country:eh, iso_3166_1: 'EH', native_name: 'Western Sahara' }, { english_name: 'Eritrea', id: country:er, iso_3166_1: 'ER', native_name: 'Eritrea' }, { english_name: 'Spain', id: country:es, iso_3166_1: 'ES', native_name: 'Spain' }, { english_name: 'Ethiopia', id: country:et, iso_3166_1: 'ET', native_name: 'Ethiopia' }, { english_name: 'Finland', id: country:fi, iso_3166_1: 'FI', native_name: 'Finland' }, { english_name: 'Fiji', id: country:fj, iso_3166_1: 'FJ', native_name: 'Fiji' }, { english_name: 'Falkland Islands', id: country:fk, iso_3166_1: 'FK', native_name: 'Falkland Islands' }, { english_name: 'Micronesia', id: country:fm, iso_3166_1: 'FM', native_name: 'Micronesia' }, { english_name: 'Faeroe Islands', id: country:fo, iso_3166_1: 'FO', native_name: 'Faroe Islands' }, { english_name: 'France', id: country:fr, iso_3166_1: 'FR', native_name: 'France' }, { english_name: 'Gabon', id: country:ga, iso_3166_1: 'GA', native_name: 'Gabon' }, { english_name: 'United Kingdom', id: country:gb, iso_3166_1: 'GB', native_name: 'United Kingdom' }, { english_name: 'Grenada', id: country:gd, iso_3166_1: 'GD', native_name: 'Grenada' }, { english_name: 'Georgia', id: country:ge, iso_3166_1: 'GE', native_name: 'Georgia' }, { english_name: 'French Guiana', id: country:gf, iso_3166_1: 'GF', native_name: 'French Guiana' }, { english_name: 'Ghana', id: country:gh, iso_3166_1: 'GH', native_name: 'Ghana' }, { english_name: 'Gibraltar', id: country:gi, iso_3166_1: 'GI', native_name: 'Gibraltar' }, { english_name: 'Greenland', id: country:gl, iso_3166_1: 'GL', native_name: 'Greenland' }, { english_name: 'Gambia', id: country:gm, iso_3166_1: 'GM', native_name: 'Gambia' }, { english_name: 'Guinea', id: country:gn, iso_3166_1: 'GN', native_name: 'Guinea' }, { english_name: 'Guadaloupe', id: country:gp, iso_3166_1: 'GP', native_name: 'Guadeloupe' }, { english_name: 'Equatorial Guinea', id: country:gq, iso_3166_1: 'GQ', native_name: 'Equatorial Guinea' }, { english_name: 'Greece', id: country:gr, iso_3166_1: 'GR', native_name: 'Greece' }, { english_name: 'South Georgia and the South Sandwich Islands', id: country:gs, iso_3166_1: 'GS', native_name: 'South Georgia & South Sandwich Islands' }, { english_name: 'Guatemala', id: country:gt, iso_3166_1: 'GT', native_name: 'Guatemala' }, { english_name: 'Guam', id: country:gu, iso_3166_1: 'GU', native_name: 'Guam' }, { english_name: 'Guinea-Bissau', id: country:gw, iso_3166_1: 'GW', native_name: 'Guinea-Bissau' }, { english_name: 'Guyana', id: country:gy, iso_3166_1: 'GY', native_name: 'Guyana' }, { english_name: 'Hong Kong', id: country:hk, iso_3166_1: 'HK', native_name: 'Hong Kong SAR China' }, { english_name: 'Heard and McDonald Islands', id: country:hm, iso_3166_1: 'HM', native_name: 'Heard & McDonald Islands' }, { english_name: 'Honduras', id: country:hn, iso_3166_1: 'HN', native_name: 'Honduras' }, { english_name: 'Croatia', id: country:hr, iso_3166_1: 'HR', native_name: 'Croatia' }, { english_name: 'Haiti', id: country:ht, iso_3166_1: 'HT', native_name: 'Haiti' }, { english_name: 'Hungary', id: country:hu, iso_3166_1: 'HU', native_name: 'Hungary' }, { english_name: 'Indonesia', id: country:id, iso_3166_1: 'ID', native_name: 'Indonesia' }, { english_name: 'Ireland', id: country:ie, iso_3166_1: 'IE', native_name: 'Ireland' }, { english_name: 'Israel', id: country:il, iso_3166_1: 'IL', native_name: 'Israel' }, { english_name: 'India', id: country:in, iso_3166_1: 'IN', native_name: 'India' }, { english_name: 'British Indian Ocean Territory', id: country:io, iso_3166_1: 'IO', native_name: 'British Indian Ocean Territory' }, { english_name: 'Iraq', id: country:iq, iso_3166_1: 'IQ', native_name: 'Iraq' }, { english_name: 'Iran', id: country:ir, iso_3166_1: 'IR', native_name: 'Iran' }, { english_name: 'Iceland', id: country:is, iso_3166_1: 'IS', native_name: 'Iceland' }, { english_name: 'Italy', id: country:it, iso_3166_1: 'IT', native_name: 'Italy' }, { english_name: 'Jamaica', id: country:jm, iso_3166_1: 'JM', native_name: 'Jamaica' }, { english_name: 'Jordan', id: country:jo, iso_3166_1: 'JO', native_name: 'Jordan' }, { english_name: 'Japan', id: country:jp, iso_3166_1: 'JP', native_name: 'Japan' }, { english_name: 'Kenya', id: country:ke, iso_3166_1: 'KE', native_name: 'Kenya' }, { english_name: 'Kyrgyz Republic', id: country:kg, iso_3166_1: 'KG', native_name: 'Kyrgyzstan' }, { english_name: 'Cambodia', id: country:kh, iso_3166_1: 'KH', native_name: 'Cambodia' }, { english_name: 'Kiribati', id: country:ki, iso_3166_1: 'KI', native_name: 'Kiribati' }, { english_name: 'Comoros', id: country:km, iso_3166_1: 'KM', native_name: 'Comoros' }, { english_name: 'St. Kitts and Nevis', id: country:kn, iso_3166_1: 'KN', native_name: 'St. Kitts & Nevis' }, { english_name: 'North Korea', id: country:kp, iso_3166_1: 'KP', native_name: 'North Korea' }, { english_name: 'South Korea', id: country:kr, iso_3166_1: 'KR', native_name: 'South Korea' }, { english_name: 'Kuwait', id: country:kw, iso_3166_1: 'KW', native_name: 'Kuwait' }, { english_name: 'Cayman Islands', id: country:ky, iso_3166_1: 'KY', native_name: 'Cayman Islands' }, { english_name: 'Kazakhstan', id: country:kz, iso_3166_1: 'KZ', native_name: 'Kazakhstan' }, { english_name: "Lao People's Democratic Republic", id: country:la, iso_3166_1: 'LA', native_name: 'Laos' }, { english_name: 'Lebanon', id: country:lb, iso_3166_1: 'LB', native_name: 'Lebanon' }, { english_name: 'St. Lucia', id: country:lc, iso_3166_1: 'LC', native_name: 'St. Lucia' }, { english_name: 'Liechtenstein', id: country:li, iso_3166_1: 'LI', native_name: 'Liechtenstein' }, { english_name: 'Sri Lanka', id: country:lk, iso_3166_1: 'LK', native_name: 'Sri Lanka' }, { english_name: 'Liberia', id: country:lr, iso_3166_1: 'LR', native_name: 'Liberia' }, { english_name: 'Lesotho', id: country:ls, iso_3166_1: 'LS', native_name: 'Lesotho' }, { english_name: 'Lithuania', id: country:lt, iso_3166_1: 'LT', native_name: 'Lithuania' }, { english_name: 'Luxembourg', id: country:lu, iso_3166_1: 'LU', native_name: 'Luxembourg' }, { english_name: 'Latvia', id: country:lv, iso_3166_1: 'LV', native_name: 'Latvia' }, { english_name: 'Libyan Arab Jamahiriya', id: country:ly, iso_3166_1: 'LY', native_name: 'Libya' }, { english_name: 'Morocco', id: country:ma, iso_3166_1: 'MA', native_name: 'Morocco' }, { english_name: 'Monaco', id: country:mc, iso_3166_1: 'MC', native_name: 'Monaco' }, { english_name: 'Moldova', id: country:md, iso_3166_1: 'MD', native_name: 'Moldova' }, { english_name: 'Montenegro', id: country:me, iso_3166_1: 'ME', native_name: 'Montenegro' }, { english_name: 'Madagascar', id: country:mg, iso_3166_1: 'MG', native_name: 'Madagascar' }, { english_name: 'Marshall Islands', id: country:mh, iso_3166_1: 'MH', native_name: 'Marshall Islands' }, { english_name: 'Macedonia', id: country:mk, iso_3166_1: 'MK', native_name: 'Macedonia' }, { english_name: 'Mali', id: country:ml, iso_3166_1: 'ML', native_name: 'Mali' }, { english_name: 'Myanmar', id: country:mm, iso_3166_1: 'MM', native_name: 'Myanmar (Burma)' }, { english_name: 'Mongolia', id: country:mn, iso_3166_1: 'MN', native_name: 'Mongolia' }, { english_name: 'Macao', id: country:mo, iso_3166_1: 'MO', native_name: 'Macau SAR China' }, { english_name: 'Northern Mariana Islands', id: country:mp, iso_3166_1: 'MP', native_name: 'Northern Mariana Islands' }, { english_name: 'Martinique', id: country:mq, iso_3166_1: 'MQ', native_name: 'Martinique' }, { english_name: 'Mauritania', id: country:mr, iso_3166_1: 'MR', native_name: 'Mauritania' }, { english_name: 'Montserrat', id: country:ms, iso_3166_1: 'MS', native_name: 'Montserrat' }, { english_name: 'Malta', id: country:mt, iso_3166_1: 'MT', native_name: 'Malta' }, { english_name: 'Mauritius', id: country:mu, iso_3166_1: 'MU', native_name: 'Mauritius' }, { english_name: 'Maldives', id: country:mv, iso_3166_1: 'MV', native_name: 'Maldives' }, { english_name: 'Malawi', id: country:mw, iso_3166_1: 'MW', native_name: 'Malawi' }, { english_name: 'Mexico', id: country:mx, iso_3166_1: 'MX', native_name: 'Mexico' }, { english_name: 'Malaysia', id: country:my, iso_3166_1: 'MY', native_name: 'Malaysia' }, { english_name: 'Mozambique', id: country:mz, iso_3166_1: 'MZ', native_name: 'Mozambique' }, { english_name: 'Namibia', id: country:na, iso_3166_1: 'NA', native_name: 'Namibia' }, { english_name: 'New Caledonia', id: country:nc, iso_3166_1: 'NC', native_name: 'New Caledonia' }, { english_name: 'Niger', id: country:ne, iso_3166_1: 'NE', native_name: 'Niger' }, { english_name: 'Norfolk Island', id: country:nf, iso_3166_1: 'NF', native_name: 'Norfolk Island' }, { english_name: 'Nigeria', id: country:ng, iso_3166_1: 'NG', native_name: 'Nigeria' }, { english_name: 'Nicaragua', id: country:ni, iso_3166_1: 'NI', native_name: 'Nicaragua' }, { english_name: 'Netherlands', id: country:nl, iso_3166_1: 'NL', native_name: 'Netherlands' }, { english_name: 'Norway', id: country:no, iso_3166_1: 'NO', native_name: 'Norway' }, { english_name: 'Nepal', id: country:np, iso_3166_1: 'NP', native_name: 'Nepal' }, { english_name: 'Nauru', id: country:nr, iso_3166_1: 'NR', native_name: 'Nauru' }, { english_name: 'Niue', id: country:nu, iso_3166_1: 'NU', native_name: 'Niue' }, { english_name: 'New Zealand', id: country:nz, iso_3166_1: 'NZ', native_name: 'New Zealand' }, { english_name: 'Oman', id: country:om, iso_3166_1: 'OM', native_name: 'Oman' }, { english_name: 'Panama', id: country:pa, iso_3166_1: 'PA', native_name: 'Panama' }, { english_name: 'Peru', id: country:pe, iso_3166_1: 'PE', native_name: 'Peru' }, { english_name: 'French Polynesia', id: country:pf, iso_3166_1: 'PF', native_name: 'French Polynesia' }, { english_name: 'Papua New Guinea', id: country:pg, iso_3166_1: 'PG', native_name: 'Papua New Guinea' }, { english_name: 'Philippines', id: country:ph, iso_3166_1: 'PH', native_name: 'Philippines' }, { english_name: 'Pakistan', id: country:pk, iso_3166_1: 'PK', native_name: 'Pakistan' }, { english_name: 'Poland', id: country:pl, iso_3166_1: 'PL', native_name: 'Poland' }, { english_name: 'St. Pierre and Miquelon', id: country:pm, iso_3166_1: 'PM', native_name: 'St. Pierre & Miquelon' }, { english_name: 'Pitcairn Island', id: country:pn, iso_3166_1: 'PN', native_name: 'Pitcairn Islands' }, { english_name: 'Puerto Rico', id: country:pr, iso_3166_1: 'PR', native_name: 'Puerto Rico' }, { english_name: 'Palestinian Territory', id: country:ps, iso_3166_1: 'PS', native_name: 'Palestinian Territories' }, { english_name: 'Portugal', id: country:pt, iso_3166_1: 'PT', native_name: 'Portugal' }, { english_name: 'Palau', id: country:pw, iso_3166_1: 'PW', native_name: 'Palau' }, { english_name: 'Paraguay', id: country:py, iso_3166_1: 'PY', native_name: 'Paraguay' }, { english_name: 'Qatar', id: country:qa, iso_3166_1: 'QA', native_name: 'Qatar' }, { english_name: 'Reunion', id: country:re, iso_3166_1: 'RE', native_name: 'Réunion' }, { english_name: 'Romania', id: country:ro, iso_3166_1: 'RO', native_name: 'Romania' }, { english_name: 'Serbia', id: country:rs, iso_3166_1: 'RS', native_name: 'Serbia' }, { english_name: 'Russia', id: country:ru, iso_3166_1: 'RU', native_name: 'Russia' }, { english_name: 'Rwanda', id: country:rw, iso_3166_1: 'RW', native_name: 'Rwanda' }, { english_name: 'Saudi Arabia', id: country:sa, iso_3166_1: 'SA', native_name: 'Saudi Arabia' }, { english_name: 'Solomon Islands', id: country:sb, iso_3166_1: 'SB', native_name: 'Solomon Islands' }, { english_name: 'Seychelles', id: country:sc, iso_3166_1: 'SC', native_name: 'Seychelles' }, { english_name: 'Sudan', id: country:sd, iso_3166_1: 'SD', native_name: 'Sudan' }, { english_name: 'Sweden', id: country:se, iso_3166_1: 'SE', native_name: 'Sweden' }, { english_name: 'Singapore', id: country:sg, iso_3166_1: 'SG', native_name: 'Singapore' }, { english_name: 'St. Helena', id: country:sh, iso_3166_1: 'SH', native_name: 'St. Helena' }, { english_name: 'Slovenia', id: country:si, iso_3166_1: 'SI', native_name: 'Slovenia' }, { english_name: 'Svalbard & Jan Mayen Islands', id: country:sj, iso_3166_1: 'SJ', native_name: 'Svalbard & Jan Mayen' }, { english_name: 'Slovakia', id: country:sk, iso_3166_1: 'SK', native_name: 'Slovakia' }, { english_name: 'Sierra Leone', id: country:sl, iso_3166_1: 'SL', native_name: 'Sierra Leone' }, { english_name: 'San Marino', id: country:sm, iso_3166_1: 'SM', native_name: 'San Marino' }, { english_name: 'Senegal', id: country:sn, iso_3166_1: 'SN', native_name: 'Senegal' }, { english_name: 'Somalia', id: country:so, iso_3166_1: 'SO', native_name: 'Somalia' }, { english_name: 'Suriname', id: country:sr, iso_3166_1: 'SR', native_name: 'Suriname' }, { english_name: 'South Sudan', id: country:ss, iso_3166_1: 'SS', native_name: 'South Sudan' }, { english_name: 'Sao Tome and Principe', id: country:st, iso_3166_1: 'ST', native_name: 'São Tomé & Príncipe' }, { english_name: 'Soviet Union', id: country:su, iso_3166_1: 'SU', native_name: 'Soviet Union' }, { english_name: 'El Salvador', id: country:sv, iso_3166_1: 'SV', native_name: 'El Salvador' }, { english_name: 'Syrian Arab Republic', id: country:sy, iso_3166_1: 'SY', native_name: 'Syria' }, { english_name: 'Swaziland', id: country:sz, iso_3166_1: 'SZ', native_name: 'Eswatini (Swaziland)' }, { english_name: 'Turks and Caicos Islands', id: country:tc, iso_3166_1: 'TC', native_name: 'Turks & Caicos Islands' }, { english_name: 'Chad', id: country:td, iso_3166_1: 'TD', native_name: 'Chad' }, { english_name: 'French Southern Territories', id: country:tf, iso_3166_1: 'TF', native_name: 'French Southern Territories' }, { english_name: 'Togo', id: country:tg, iso_3166_1: 'TG', native_name: 'Togo' }, { english_name: 'Thailand', id: country:th, iso_3166_1: 'TH', native_name: 'Thailand' }, { english_name: 'Tajikistan', id: country:tj, iso_3166_1: 'TJ', native_name: 'Tajikistan' }, { english_name: 'Tokelau', id: country:tk, iso_3166_1: 'TK', native_name: 'Tokelau' }, { english_name: 'Timor-Leste', id: country:tl, iso_3166_1: 'TL', native_name: 'Timor-Leste' }, { english_name: 'Turkmenistan', id: country:tm, iso_3166_1: 'TM', native_name: 'Turkmenistan' }, { english_name: 'Tunisia', id: country:tn, iso_3166_1: 'TN', native_name: 'Tunisia' }, { english_name: 'Tonga', id: country:to, iso_3166_1: 'TO', native_name: 'Tonga' }, { english_name: 'East Timor', id: country:tp, iso_3166_1: 'TP', native_name: 'East Timor' }, { english_name: 'Turkey', id: country:tr, iso_3166_1: 'TR', native_name: 'Turkey' }, { english_name: 'Trinidad and Tobago', id: country:tt, iso_3166_1: 'TT', native_name: 'Trinidad & Tobago' }, { english_name: 'Tuvalu', id: country:tv, iso_3166_1: 'TV', native_name: 'Tuvalu' }, { english_name: 'Taiwan', id: country:tw, iso_3166_1: 'TW', native_name: 'Taiwan' }, { english_name: 'Tanzania', id: country:tz, iso_3166_1: 'TZ', native_name: 'Tanzania' }, { english_name: 'Ukraine', id: country:ua, iso_3166_1: 'UA', native_name: 'Ukraine' }, { english_name: 'Uganda', id: country:ug, iso_3166_1: 'UG', native_name: 'Uganda' }, { english_name: 'United States Minor Outlying Islands', id: country:um, iso_3166_1: 'UM', native_name: 'U.S. Outlying Islands' }, { english_name: 'United States of America', id: country:us, iso_3166_1: 'US', native_name: 'United States' }, { english_name: 'Uruguay', id: country:uy, iso_3166_1: 'UY', native_name: 'Uruguay' }, { english_name: 'Uzbekistan', id: country:uz, iso_3166_1: 'UZ', native_name: 'Uzbekistan' }, { english_name: 'Holy See', id: country:va, iso_3166_1: 'VA', native_name: 'Vatican City' }, { english_name: 'St. Vincent and the Grenadines', id: country:vc, iso_3166_1: 'VC', native_name: 'St. Vincent & Grenadines' }, { english_name: 'Venezuela', id: country:ve, iso_3166_1: 'VE', native_name: 'Venezuela' }, { english_name: 'British Virgin Islands', id: country:vg, iso_3166_1: 'VG', native_name: 'British Virgin Islands' }, { english_name: 'US Virgin Islands', id: country:vi, iso_3166_1: 'VI', native_name: 'U.S. Virgin Islands' }, { english_name: 'Vietnam', id: country:vn, iso_3166_1: 'VN', native_name: 'Vietnam' }, { english_name: 'Vanuatu', id: country:vu, iso_3166_1: 'VU', native_name: 'Vanuatu' }, { english_name: 'Wallis and Futuna Islands', id: country:wf, iso_3166_1: 'WF', native_name: 'Wallis & Futuna' }, { english_name: 'Samoa', id: country:ws, iso_3166_1: 'WS', native_name: 'Samoa' }, { english_name: 'Czechoslovakia', id: country:xc, iso_3166_1: 'XC', native_name: 'Czechoslovakia' }, { english_name: 'East Germany', id: country:xg, iso_3166_1: 'XG', native_name: 'East Germany' }, { english_name: 'Northern Ireland', id: country:xi, iso_3166_1: 'XI', native_name: 'Northern Ireland' }, { english_name: 'Kosovo', id: country:xk, iso_3166_1: 'XK', native_name: 'Kosovo' }, { english_name: 'Yemen', id: country:ye, iso_3166_1: 'YE', native_name: 'Yemen' }, { english_name: 'Mayotte', id: country:yt, iso_3166_1: 'YT', native_name: 'Mayotte' }, { english_name: 'Yugoslavia', id: country:yu, iso_3166_1: 'YU', native_name: 'Yugoslavia' }, { english_name: 'South Africa', id: country:za, iso_3166_1: 'ZA', native_name: 'South Africa' }, { english_name: 'Zambia', id: country:zm, iso_3166_1: 'ZM', native_name: 'Zambia' }, { english_name: 'Zaire', id: country:zr, iso_3166_1: 'ZR', native_name: 'Zaire' }, { english_name: 'Zimbabwe', id: country:zw, iso_3166_1: 'ZW', native_name: 'Zimbabwe' } ];

-- ------------------------------
-- TABLE: featured
-- ------------------------------

DEFINE TABLE featured TYPE RELATION IN music OUT movie | show SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD in ON featured TYPE record<music> PERMISSIONS FULL;
DEFINE FIELD out ON featured TYPE record<movie | `show`> PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: featured
-- ------------------------------


-- ------------------------------
-- TABLE: genre
-- ------------------------------

DEFINE TABLE genre TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete WHERE NONE;

DEFINE FIELD id ON genre TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON genre TYPE string PERMISSIONS FULL;

DEFINE INDEX id_idx ON genre FIELDS id UNIQUE CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: genre
-- ------------------------------

INSERT [ { id: genre:0, name: 'empty' }, { id: genre:12, name: 'Adventure' }, { id: genre:14, name: 'Fantasy' }, { id: genre:16, name: 'Animation' }, { id: genre:18, name: 'Drama' }, { id: genre:27, name: 'Horror' }, { id: genre:28, name: 'Action' }, { id: genre:35, name: 'Comedy' }, { id: genre:36, name: 'History' }, { id: genre:37, name: 'Western' }, { id: genre:53, name: 'Thriller' }, { id: genre:80, name: 'Crime' }, { id: genre:99, name: 'Documentary' }, { id: genre:878, name: 'Science Fiction' }, { id: genre:9648, name: 'Mystery' }, { id: genre:10402, name: 'Music' }, { id: genre:10749, name: 'Romance' }, { id: genre:10751, name: 'Family' }, { id: genre:10752, name: 'War' }, { id: genre:10759, name: 'Action & Adventure' }, { id: genre:10762, name: 'Kids' }, { id: genre:10763, name: 'News' }, { id: genre:10764, name: 'Reality' }, { id: genre:10765, name: 'Sci-Fi & Fantasy' }, { id: genre:10766, name: 'Soap' }, { id: genre:10767, name: 'Talk' }, { id: genre:10768, name: 'War & Politics' }, { id: genre:10770, name: 'TV Movie' } ];

-- ------------------------------
-- TABLE: language
-- ------------------------------

DEFINE TABLE language TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD english_name ON language TYPE string PERMISSIONS FULL;
DEFINE FIELD id ON language TYPE string PERMISSIONS FULL;
DEFINE FIELD iso_639_1 ON language TYPE string PERMISSIONS FULL;
DEFINE FIELD name ON language TYPE string PERMISSIONS FULL;

DEFINE INDEX id_idx ON language FIELDS id UNIQUE CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: language
-- ------------------------------

INSERT [ { english_name: 'Afar', id: language:aa, iso_639_1: 'aa', name: '' }, { english_name: 'Abkhazian', id: language:ab, iso_639_1: 'ab', name: '' }, { english_name: 'Avestan', id: language:ae, iso_639_1: 'ae', name: '' }, { english_name: 'Afrikaans', id: language:af, iso_639_1: 'af', name: 'Afrikaans' }, { english_name: 'Akan', id: language:ak, iso_639_1: 'ak', name: '' }, { english_name: 'Amharic', id: language:am, iso_639_1: 'am', name: '' }, { english_name: 'Aragonese', id: language:an, iso_639_1: 'an', name: '' }, { english_name: 'Arabic', id: language:ar, iso_639_1: 'ar', name: 'العربية' }, { english_name: 'Assamese', id: language:as, iso_639_1: 'as', name: '' }, { english_name: 'Avaric', id: language:av, iso_639_1: 'av', name: '' }, { english_name: 'Aymara', id: language:ay, iso_639_1: 'ay', name: '' }, { english_name: 'Azerbaijani', id: language:az, iso_639_1: 'az', name: 'Azərbaycan' }, { english_name: 'Bashkir', id: language:ba, iso_639_1: 'ba', name: '' }, { english_name: 'Belarusian', id: language:be, iso_639_1: 'be', name: 'беларуская мова' }, { english_name: 'Bulgarian', id: language:bg, iso_639_1: 'bg', name: 'български език' }, { english_name: 'Bislama', id: language:bi, iso_639_1: 'bi', name: '' }, { english_name: 'Bambara', id: language:bm, iso_639_1: 'bm', name: 'Bamanankan' }, { english_name: 'Bengali', id: language:bn, iso_639_1: 'bn', name: 'বাংলা' }, { english_name: 'Tibetan', id: language:bo, iso_639_1: 'bo', name: '' }, { english_name: 'Breton', id: language:br, iso_639_1: 'br', name: '' }, { english_name: 'Bosnian', id: language:bs, iso_639_1: 'bs', name: 'Bosanski' }, { english_name: 'Catalan', id: language:ca, iso_639_1: 'ca', name: 'Català' }, { english_name: 'Chechen', id: language:ce, iso_639_1: 'ce', name: '' }, { english_name: 'Chamorro', id: language:ch, iso_639_1: 'ch', name: "Finu' Chamorro" }, { english_name: 'Cantonese', id: language:cn, iso_639_1: 'cn', name: '广州话 / 廣州話' }, { english_name: 'Corsican', id: language:co, iso_639_1: 'co', name: '' }, { english_name: 'Cree', id: language:cr, iso_639_1: 'cr', name: '' }, { english_name: 'Czech', id: language:cs, iso_639_1: 'cs', name: 'Český' }, { english_name: 'Slavic', id: language:cu, iso_639_1: 'cu', name: '' }, { english_name: 'Chuvash', id: language:cv, iso_639_1: 'cv', name: '' }, { english_name: 'Welsh', id: language:cy, iso_639_1: 'cy', name: 'Cymraeg' }, { english_name: 'Danish', id: language:da, iso_639_1: 'da', name: 'Dansk' }, { english_name: 'German', id: language:de, iso_639_1: 'de', name: 'Deutsch' }, { english_name: 'Divehi', id: language:dv, iso_639_1: 'dv', name: '' }, { english_name: 'Dzongkha', id: language:dz, iso_639_1: 'dz', name: '' }, { english_name: 'Ewe', id: language:ee, iso_639_1: 'ee', name: 'Èʋegbe' }, { english_name: 'Greek', id: language:el, iso_639_1: 'el', name: 'ελληνικά' }, { english_name: 'English', id: language:en, iso_639_1: 'en', name: 'English' }, { english_name: 'Esperanto', id: language:eo, iso_639_1: 'eo', name: 'Esperanto' }, { english_name: 'Spanish', id: language:es, iso_639_1: 'es', name: 'Español' }, { english_name: 'Estonian', id: language:et, iso_639_1: 'et', name: 'Eesti' }, { english_name: 'Basque', id: language:eu, iso_639_1: 'eu', name: 'euskera' }, { english_name: 'Persian', id: language:fa, iso_639_1: 'fa', name: 'فارسی' }, { english_name: 'Fulah', id: language:ff, iso_639_1: 'ff', name: 'Fulfulde' }, { english_name: 'Finnish', id: language:fi, iso_639_1: 'fi', name: 'suomi' }, { english_name: 'Fijian', id: language:fj, iso_639_1: 'fj', name: '' }, { english_name: 'Faroese', id: language:fo, iso_639_1: 'fo', name: '' }, { english_name: 'French', id: language:fr, iso_639_1: 'fr', name: 'Français' }, { english_name: 'Frisian', id: language:fy, iso_639_1: 'fy', name: '' }, { english_name: 'Irish', id: language:ga, iso_639_1: 'ga', name: 'Gaeilge' }, { english_name: 'Gaelic', id: language:gd, iso_639_1: 'gd', name: '' }, { english_name: 'Galician', id: language:gl, iso_639_1: 'gl', name: 'Galego' }, { english_name: 'Guarani', id: language:gn, iso_639_1: 'gn', name: '' }, { english_name: 'Gujarati', id: language:gu, iso_639_1: 'gu', name: '' }, { english_name: 'Manx', id: language:gv, iso_639_1: 'gv', name: '' }, { english_name: 'Hausa', id: language:ha, iso_639_1: 'ha', name: 'Hausa' }, { english_name: 'Hebrew', id: language:he, iso_639_1: 'he', name: 'עִבְרִית' }, { english_name: 'Hindi', id: language:hi, iso_639_1: 'hi', name: 'हिन्दी' }, { english_name: 'Hiri Motu', id: language:ho, iso_639_1: 'ho', name: '' }, { english_name: 'Croatian', id: language:hr, iso_639_1: 'hr', name: 'Hrvatski' }, { english_name: 'Haitian; Haitian Creole', id: language:ht, iso_639_1: 'ht', name: '' }, { english_name: 'Hungarian', id: language:hu, iso_639_1: 'hu', name: 'Magyar' }, { english_name: 'Armenian', id: language:hy, iso_639_1: 'hy', name: '' }, { english_name: 'Herero', id: language:hz, iso_639_1: 'hz', name: '' }, { english_name: 'Interlingua', id: language:ia, iso_639_1: 'ia', name: '' }, { english_name: 'Indonesian', id: language:id, iso_639_1: 'id', name: 'Bahasa indonesia' }, { english_name: 'Interlingue', id: language:ie, iso_639_1: 'ie', name: '' }, { english_name: 'Igbo', id: language:ig, iso_639_1: 'ig', name: '' }, { english_name: 'Yi', id: language:ii, iso_639_1: 'ii', name: '' }, { english_name: 'Inupiaq', id: language:ik, iso_639_1: 'ik', name: '' }, { english_name: 'Ido', id: language:io, iso_639_1: 'io', name: '' }, { english_name: 'Icelandic', id: language:is, iso_639_1: 'is', name: 'Íslenska' }, { english_name: 'Italian', id: language:it, iso_639_1: 'it', name: 'Italiano' }, { english_name: 'Inuktitut', id: language:iu, iso_639_1: 'iu', name: '' }, { english_name: 'Japanese', id: language:ja, iso_639_1: 'ja', name: '日本語' }, { english_name: 'Javanese', id: language:jv, iso_639_1: 'jv', name: '' }, { english_name: 'Georgian', id: language:ka, iso_639_1: 'ka', name: 'ქართული' }, { english_name: 'Kongo', id: language:kg, iso_639_1: 'kg', name: '' }, { english_name: 'Kikuyu', id: language:ki, iso_639_1: 'ki', name: '' }, { english_name: 'Kuanyama', id: language:kj, iso_639_1: 'kj', name: '' }, { english_name: 'Kazakh', id: language:kk, iso_639_1: 'kk', name: 'қазақ' }, { english_name: 'Kalaallisut', id: language:kl, iso_639_1: 'kl', name: '' }, { english_name: 'Khmer', id: language:km, iso_639_1: 'km', name: '' }, { english_name: 'Kannada', id: language:kn, iso_639_1: 'kn', name: '?????' }, { english_name: 'Korean', id: language:ko, iso_639_1: 'ko', name: '한국어/조선말' }, { english_name: 'Kanuri', id: language:kr, iso_639_1: 'kr', name: '' }, { english_name: 'Kashmiri', id: language:ks, iso_639_1: 'ks', name: '' }, { english_name: 'Kurdish', id: language:ku, iso_639_1: 'ku', name: '' }, { english_name: 'Komi', id: language:kv, iso_639_1: 'kv', name: '' }, { english_name: 'Cornish', id: language:kw, iso_639_1: 'kw', name: '' }, { english_name: 'Kirghiz', id: language:ky, iso_639_1: 'ky', name: '??????' }, { english_name: 'Latin', id: language:la, iso_639_1: 'la', name: 'Latin' }, { english_name: 'Letzeburgesch', id: language:lb, iso_639_1: 'lb', name: '' }, { english_name: 'Ganda', id: language:lg, iso_639_1: 'lg', name: '' }, { english_name: 'Limburgish', id: language:li, iso_639_1: 'li', name: '' }, { english_name: 'Lingala', id: language:ln, iso_639_1: 'ln', name: '' }, { english_name: 'Lao', id: language:lo, iso_639_1: 'lo', name: '' }, { english_name: 'Lithuanian', id: language:lt, iso_639_1: 'lt', name: 'Lietuvių' }, { english_name: 'Luba-Katanga', id: language:lu, iso_639_1: 'lu', name: '' }, { english_name: 'Latvian', id: language:lv, iso_639_1: 'lv', name: 'Latviešu' }, { english_name: 'Malagasy', id: language:mg, iso_639_1: 'mg', name: '' }, { english_name: 'Marshall', id: language:mh, iso_639_1: 'mh', name: '' }, { english_name: 'Maori', id: language:mi, iso_639_1: 'mi', name: '' }, { english_name: 'Macedonian', id: language:mk, iso_639_1: 'mk', name: '' }, { english_name: 'Malayalam', id: language:ml, iso_639_1: 'ml', name: '' }, { english_name: 'Mongolian', id: language:mn, iso_639_1: 'mn', name: '' }, { english_name: 'Moldavian', id: language:mo, iso_639_1: 'mo', name: '' }, { english_name: 'Marathi', id: language:mr, iso_639_1: 'mr', name: '' }, { english_name: 'Malay', id: language:ms, iso_639_1: 'ms', name: 'Bahasa melayu' }, { english_name: 'Maltese', id: language:mt, iso_639_1: 'mt', name: 'Malti' }, { english_name: 'Burmese', id: language:my, iso_639_1: 'my', name: '' }, { english_name: 'Nauru', id: language:na, iso_639_1: 'na', name: '' }, { english_name: 'Norwegian Bokmål', id: language:nb, iso_639_1: 'nb', name: 'Bokmål' }, { english_name: 'Ndebele', id: language:nd, iso_639_1: 'nd', name: '' }, { english_name: 'Nepali', id: language:ne, iso_639_1: 'ne', name: '' }, { english_name: 'Ndonga', id: language:ng, iso_639_1: 'ng', name: '' }, { english_name: 'Dutch', id: language:nl, iso_639_1: 'nl', name: 'Nederlands' }, { english_name: 'Norwegian Nynorsk', id: language:nn, iso_639_1: 'nn', name: '' }, { english_name: 'Norwegian', id: language:no, iso_639_1: 'no', name: 'Norsk' }, { english_name: 'Ndebele', id: language:nr, iso_639_1: 'nr', name: '' }, { english_name: 'Navajo', id: language:nv, iso_639_1: 'nv', name: '' }, { english_name: 'Chichewa; Nyanja', id: language:ny, iso_639_1: 'ny', name: '' }, { english_name: 'Occitan', id: language:oc, iso_639_1: 'oc', name: '' }, { english_name: 'Ojibwa', id: language:oj, iso_639_1: 'oj', name: '' }, { english_name: 'Oromo', id: language:om, iso_639_1: 'om', name: '' }, { english_name: 'Oriya', id: language:or, iso_639_1: 'or', name: '' }, { english_name: 'Ossetian; Ossetic', id: language:os, iso_639_1: 'os', name: '' }, { english_name: 'Punjabi', id: language:pa, iso_639_1: 'pa', name: 'ਪੰਜਾਬੀ' }, { english_name: 'Pali', id: language:pi, iso_639_1: 'pi', name: '' }, { english_name: 'Polish', id: language:pl, iso_639_1: 'pl', name: 'Polski' }, { english_name: 'Pushto', id: language:ps, iso_639_1: 'ps', name: 'پښتو' }, { english_name: 'Portuguese', id: language:pt, iso_639_1: 'pt', name: 'Português' }, { english_name: 'Quechua', id: language:qu, iso_639_1: 'qu', name: '' }, { english_name: 'Raeto-Romance', id: language:rm, iso_639_1: 'rm', name: '' }, { english_name: 'Rundi', id: language:rn, iso_639_1: 'rn', name: 'Kirundi' }, { english_name: 'Romanian', id: language:ro, iso_639_1: 'ro', name: 'Română' }, { english_name: 'Russian', id: language:ru, iso_639_1: 'ru', name: 'Pусский' }, { english_name: 'Kinyarwanda', id: language:rw, iso_639_1: 'rw', name: 'Kinyarwanda' }, { english_name: 'Sanskrit', id: language:sa, iso_639_1: 'sa', name: '' }, { english_name: 'Sardinian', id: language:sc, iso_639_1: 'sc', name: '' }, { english_name: 'Sindhi', id: language:sd, iso_639_1: 'sd', name: '' }, { english_name: 'Northern Sami', id: language:se, iso_639_1: 'se', name: '' }, { english_name: 'Sango', id: language:sg, iso_639_1: 'sg', name: '' }, { english_name: 'Serbo-Croatian', id: language:sh, iso_639_1: 'sh', name: '' }, { english_name: 'Sinhalese', id: language:si, iso_639_1: 'si', name: 'සිංහල' }, { english_name: 'Slovak', id: language:sk, iso_639_1: 'sk', name: 'Slovenčina' }, { english_name: 'Slovenian', id: language:sl, iso_639_1: 'sl', name: 'Slovenščina' }, { english_name: 'Samoan', id: language:sm, iso_639_1: 'sm', name: '' }, { english_name: 'Shona', id: language:sn, iso_639_1: 'sn', name: '' }, { english_name: 'Somali', id: language:so, iso_639_1: 'so', name: 'Somali' }, { english_name: 'Albanian', id: language:sq, iso_639_1: 'sq', name: 'shqip' }, { english_name: 'Serbian', id: language:sr, iso_639_1: 'sr', name: 'Srpski' }, { english_name: 'Swati', id: language:ss, iso_639_1: 'ss', name: '' }, { english_name: 'Sotho', id: language:st, iso_639_1: 'st', name: '' }, { english_name: 'Sundanese', id: language:su, iso_639_1: 'su', name: '' }, { english_name: 'Swedish', id: language:sv, iso_639_1: 'sv', name: 'svenska' }, { english_name: 'Swahili', id: language:sw, iso_639_1: 'sw', name: 'Kiswahili' }, { english_name: 'Tamil', id: language:ta, iso_639_1: 'ta', name: 'தமிழ்' }, { english_name: 'Telugu', id: language:te, iso_639_1: 'te', name: 'తెలుగు' }, { english_name: 'Tajik', id: language:tg, iso_639_1: 'tg', name: '' }, { english_name: 'Thai', id: language:th, iso_639_1: 'th', name: 'ภาษาไทย' }, { english_name: 'Tigrinya', id: language:ti, iso_639_1: 'ti', name: '' }, { english_name: 'Turkmen', id: language:tk, iso_639_1: 'tk', name: '' }, { english_name: 'Tagalog', id: language:tl, iso_639_1: 'tl', name: '' }, { english_name: 'Tswana', id: language:tn, iso_639_1: 'tn', name: '' }, { english_name: 'Tonga', id: language:to, iso_639_1: 'to', name: '' }, { english_name: 'Turkish', id: language:tr, iso_639_1: 'tr', name: 'Türkçe' }, { english_name: 'Tsonga', id: language:ts, iso_639_1: 'ts', name: '' }, { english_name: 'Tatar', id: language:tt, iso_639_1: 'tt', name: '' }, { english_name: 'Twi', id: language:tw, iso_639_1: 'tw', name: '' }, { english_name: 'Tahitian', id: language:ty, iso_639_1: 'ty', name: '' }, { english_name: 'Uighur', id: language:ug, iso_639_1: 'ug', name: '' }, { english_name: 'Ukrainian', id: language:uk, iso_639_1: 'uk', name: 'Український' }, { english_name: 'Urdu', id: language:ur, iso_639_1: 'ur', name: 'اردو' }, { english_name: 'Uzbek', id: language:uz, iso_639_1: 'uz', name: 'ozbek' }, { english_name: 'Venda', id: language:ve, iso_639_1: 've', name: '' }, { english_name: 'Vietnamese', id: language:vi, iso_639_1: 'vi', name: 'Tiếng Việt' }, { english_name: 'Volapük', id: language:vo, iso_639_1: 'vo', name: '' }, { english_name: 'Walloon', id: language:wa, iso_639_1: 'wa', name: '' }, { english_name: 'Wolof', id: language:wo, iso_639_1: 'wo', name: 'Wolof' }, { english_name: 'Xhosa', id: language:xh, iso_639_1: 'xh', name: '' }, { english_name: 'No Language', id: language:xx, iso_639_1: 'xx', name: 'No Language' }, { english_name: 'Yiddish', id: language:yi, iso_639_1: 'yi', name: '' }, { english_name: 'Yoruba', id: language:yo, iso_639_1: 'yo', name: 'Èdè Yorùbá' }, { english_name: 'Zhuang', id: language:za, iso_639_1: 'za', name: '' }, { english_name: 'Mandarin', id: language:zh, iso_639_1: 'zh', name: '普通话' }, { english_name: 'Zulu', id: language:zu, iso_639_1: 'zu', name: 'isiZulu' } ];

-- ------------------------------
-- TABLE: movie
-- ------------------------------

DEFINE TABLE movie TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD budget ON movie TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD collection ON movie TYPE record<collection> | null DEFAULT NULL PERMISSIONS FULL;
DEFINE FIELD genre ON movie TYPE array<record<genre>> PERMISSIONS FULL;
DEFINE FIELD genre[*] ON movie TYPE record<genre> PERMISSIONS FULL;
DEFINE FIELD id ON movie TYPE int PERMISSIONS FULL;
DEFINE FIELD language ON movie TYPE record<language> PERMISSIONS FULL;
DEFINE FIELD overview ON movie TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD poster ON movie TYPE string DEFAULT '/empty' PERMISSIONS FULL;
DEFINE FIELD release_date ON movie TYPE datetime DEFAULT d'0000-01-01T00:00:00Z' PERMISSIONS FULL;
DEFINE FIELD revenue ON movie TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD runtime ON movie TYPE int DEFAULT 0 PERMISSIONS FULL;
DEFINE FIELD tagline ON movie TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD title ON movie TYPE string PERMISSIONS FULL;
DEFINE FIELD vote_average ON movie TYPE decimal DEFAULT 0f PERMISSIONS FULL;

DEFINE INDEX id_idx ON movie FIELDS id UNIQUE CONCURRENTLY;
DEFINE INDEX title_idx ON movie FIELDS title SEARCH ANALYZER full_text BM25(1.2,0.75) DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: movie
-- ------------------------------


-- ------------------------------
-- TABLE: music
-- ------------------------------

DEFINE TABLE music TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD title ON music TYPE string PERMISSIONS FULL;
DEFINE FIELD url ON music TYPE string PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: music
-- ------------------------------


-- ------------------------------
-- TABLE: performed
-- ------------------------------

DEFINE TABLE performed TYPE RELATION IN person OUT music SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD composer ON performed TYPE bool DEFAULT false PERMISSIONS FULL;
DEFINE FIELD in ON performed TYPE record<person> PERMISSIONS FULL;
DEFINE FIELD out ON performed TYPE record<music> PERMISSIONS FULL;
DEFINE FIELD singer ON performed TYPE bool DEFAULT false PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: performed
-- ------------------------------


-- ------------------------------
-- TABLE: person
-- ------------------------------

DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD age ON person TYPE any DEFAULT <future> { fn::get_age(birth, death) } VALUE <future> { fn::get_age(birth, death) } PERMISSIONS FOR select, update FULL, FOR create NONE;
DEFINE FIELD biography ON person TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD birth ON person TYPE datetime PERMISSIONS FULL;
DEFINE FIELD death ON person TYPE datetime DEFAULT d'0000-01-01T00:00:00Z' PERMISSIONS FULL;
DEFINE FIELD name ON person TYPE string PERMISSIONS FULL;
DEFINE FIELD popularity ON person TYPE decimal DEFAULT 0f PERMISSIONS FULL;

DEFINE INDEX date_idx ON person FIELDS birth, death CONCURRENTLY;
DEFINE INDEX popularity_idx ON person FIELDS popularity CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: person
-- ------------------------------


-- ------------------------------
-- TABLE: produced
-- ------------------------------

DEFINE TABLE produced TYPE RELATION IN company | country OUT movie | show SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD in ON produced TYPE record<company | country> PERMISSIONS FULL;
DEFINE FIELD out ON produced TYPE record<movie | `show`> PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: produced
-- ------------------------------


-- ------------------------------
-- TABLE: `show`
-- ------------------------------

DEFINE TABLE `show` TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD first_air_date ON `show` TYPE datetime DEFAULT d'0000-01-01T00:00:00Z' PERMISSIONS FULL;
DEFINE FIELD genre ON `show` TYPE array<record<genre>> PERMISSIONS FULL;
DEFINE FIELD genre[*] ON `show` TYPE record<genre> PERMISSIONS FULL;
DEFINE FIELD id ON `show` TYPE int PERMISSIONS FULL;
DEFINE FIELD language ON `show` TYPE array<record<language>> PERMISSIONS FULL;
DEFINE FIELD language[*] ON `show` TYPE record<language> PERMISSIONS FULL;
DEFINE FIELD overview ON `show` TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD poster ON `show` TYPE string ASSERT string::is::url($value) PERMISSIONS FULL;
DEFINE FIELD seasons ON `show` TYPE int DEFAULT 1 VALUE $value > 0 PERMISSIONS FULL;
DEFINE FIELD tagline ON `show` TYPE string DEFAULT '' PERMISSIONS FULL;
DEFINE FIELD title ON `show` TYPE string PERMISSIONS FULL;
DEFINE FIELD vote_average ON `show` TYPE decimal DEFAULT 0f PERMISSIONS FULL;

DEFINE INDEX id_idx ON `show` FIELDS id UNIQUE CONCURRENTLY;
DEFINE INDEX title_idx ON `show` FIELDS title SEARCH ANALYZER full_text BM25(1.2,0.75) DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: `show`
-- ------------------------------


-- ------------------------------
-- TABLE: user
-- ------------------------------

DEFINE TABLE user TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select, update, delete WHERE id = $auth.id, FOR create NONE;

DEFINE FIELD email ON user TYPE string ASSERT string::is::email($value) PERMISSIONS FULL;
DEFINE FIELD name ON user TYPE string PERMISSIONS FULL;
DEFINE FIELD password ON user TYPE string PERMISSIONS FULL;

DEFINE INDEX email ON user FIELDS email UNIQUE CONCURRENTLY;


-- ------------------------------
-- TABLE DATA: user
-- ------------------------------


-- ------------------------------
-- TABLE: worked
-- ------------------------------

DEFINE TABLE worked TYPE RELATION IN person OUT movie | show SCHEMAFULL PERMISSIONS FOR select FULL, FOR create, update, delete NONE;

DEFINE FIELD department ON worked TYPE string PERMISSIONS FULL;
DEFINE FIELD in ON worked TYPE record<person> PERMISSIONS FULL;
DEFINE FIELD job ON worked TYPE string PERMISSIONS FULL;
DEFINE FIELD out ON worked TYPE record<movie | `show`> PERMISSIONS FULL;



-- ------------------------------
-- TABLE DATA: worked
-- ------------------------------
