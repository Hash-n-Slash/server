const fs = require('fs');

class Database {
    constructor(dbDir) {
        this.dbDir = dbDir;
        if (!fs.existsSync(dbDir)) {
            fs.mkdirSync(dbDir);
        }
    }

    createTable(tableName, columns) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        if (!fs.existsSync(tableFile)) {
            const header = Object.keys(columns).join(',') + '\n';
            fs.writeFileSync(tableFile, header);
            return true;
        } else {
            throw new Error("Table already exists");
        }
    }

    insertRecord(tableName, data) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const line = Object.values(data).join(',') + '\n';
        fs.appendFileSync(tableFile, line);
        return true;
    }

    getAllRecords(tableName, page = 1, perPage = 10) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const lines = fs.readFileSync(tableFile, 'utf8').split('\n');
        const totalRecords = lines.length - 1;
        const totalPages = Math.ceil(totalRecords / perPage);
        const offset = (page - 1) * perPage;
        const records = lines.slice(offset, offset + perPage).map(line => line.split(','));
        return {
            data: records,
            totalPages: totalPages,
            currentPage: page,
            totalRecords: totalRecords
        };
    }

    getRecordById(tableName, id) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const lines = fs.readFileSync(tableFile, 'utf8').split('\n');
        for (const line of lines) {
            const data = line.split(',');
            if (data[0] === id.toString()) {
                return data;
            }
        }
        return null; // Record not found
    }

    updateRecord(tableName, id, data) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const lines = fs.readFileSync(tableFile, 'utf8').split('\n');
        const updatedLines = lines.map(line => {
            const rowData = line.split(',');
            if (rowData[0] === id.toString()) {
                return Object.values(data).join(',');
            } else {
                return line;
            }
        });
        fs.writeFileSync(tableFile, updatedLines.join('\n'));
        return true;
    }

    deleteRecord(tableName, id) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const lines = fs.readFileSync(tableFile, 'utf8').split('\n');
        const filteredLines = lines.filter(line => {
            const rowData = line.split(',');
            return rowData[0] !== id.toString();
        });
        fs.writeFileSync(tableFile, filteredLines.join('\n'));
        return true;
    }

    filterRecords(tableName, conditions, page = 1, perPage = 10, orderBy = null) {
        this.validateTableName(tableName);
        const tableFile = `${this.dbDir}/${tableName}.csv`;
        const lines = fs.readFileSync(tableFile, 'utf8').split('\n');
        const filteredData = lines.filter(line => {
            const rowData = line.split(',');
            return Object.entries(conditions).every(([column, value]) => rowData.includes(value.toString()));
        });
        let sortedData = filteredData;
        if (orderBy !== null) {
            const [columnToSort, order] = Object.entries(orderBy)[0];
            sortedData = filteredData.sort((a, b) => {
                const indexA = a.split(',').indexOf(orderBy[columnToSort]);
                const indexB = b.split(',').indexOf(orderBy[columnToSort]);
                return order === 'DESC' ? indexB - indexA : indexA - indexB;
            });
        }
        const totalRecords = sortedData.length;
        const totalPages = Math.ceil(totalRecords / perPage);
        const offset = (page - 1) * perPage;
        const paginatedData = sortedData.slice(offset, offset + perPage).map(line => line.split(','));
        return {
            data: paginatedData,
            totalPages: totalPages,
            currentPage: page,
            totalRecords: totalRecords
        };
    }

    validateTableName(tableName) {
        if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
            throw new Error("Invalid table name");
        }
    }
}

class Logger {
    static logError(message) {
        console.error(message);
    }
}

