module.exports = async (pool) => {
	const tableName = process.env.DB_TABLE_NAME;
	return {
		createTableIfNotExists: async function () {
			const queryText = `
            CREATE TABLE IF NOT EXISTS ${tableName} (
                "name" varchar NOT NULL,
                age int4 NOT NULL,
                address jsonb NULL,
                additional_info jsonb NULL,
                id serial4 NOT NULL PRIMARY KEY
            );
          `;
			return await pool.query(queryText);
		},
		insertData: async function (data) {
			const additional_info = { ...data }; // Creating a new object to avoid mutating the original
			delete additional_info.name;
			delete additional_info.age;
			delete additional_info.address;

			const name = `${data.name.firstName} ${data.name.lastName}`;
			const queryText = `
            INSERT INTO ${tableName} (name, age, address, additional_info)
            VALUES ('${name}', '${data.age}', '${JSON.stringify(
				data.address,
			)}', '${JSON.stringify(additional_info)}');
          `;
			return await pool.query(queryText);
		},
		getAgeDistribution: async function () {
			const queryText = `SELECT
            SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_less_than_20,
            SUM(CASE WHEN age >= 20 AND age <= 40 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_20_to_40,
            SUM(CASE WHEN age > 40 AND age <= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_40_to_60,
            SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_greater_than_60
        FROM ${tableName}`;
			return await pool.query(queryText);
		},
	};
};
