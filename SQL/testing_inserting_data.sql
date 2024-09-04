--INSERT INTO transaction_type (id) VALUES 
--(1)

/*
I keep getting this error: 
ERROR:  Array value must start with "{" or dimension information.malformed array literal: "testing" 

ERROR:  malformed array literal: "testing"
SQL state: 22P02
Detail: Array value must start with "{" or dimension information.
Character: 92

Could this be becasue of the Text data type? Should I change back to VarChar? 
*/

UPDATE transaction_type
SET type_name = 'testing'
WHERE id = 1;

SELECT * from transaction_type