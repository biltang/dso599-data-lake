SELECT AVG(bank_balance) as avg_bal
FROM TABLENAME
WHERE state = 'CA'
AND YEAR(DATE(date_of_birth)) BETWEEN 1997 AND 1999