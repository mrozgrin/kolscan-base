import mysql, { type OkPacket, type RowDataPacket, type ResultSetHeader, type FieldPacket } from 'mysql2/promise';
import { config } from '../config';
import { logger } from '../utils/logger';

let pool: mysql.Pool | null = null;

/**
 * Retorna o pool de conexões MySQL (singleton)
 */
export function getPool(): mysql.Pool {
  if (!pool) {
    pool = mysql.createPool({
      host: config.database.host,
      port: config.database.port,
      database: config.database.name,
      user: config.database.user,
      password: config.database.password,
      ssl: config.database.ssl ? { rejectUnauthorized: false } : undefined,
      waitForConnections: true,
      connectionLimit: config.database.poolMax,
      queueLimit: 0,
      timezone: '+00:00',
      dateStrings: false,
      decimalNumbers: true,
    });

    logger.info('MySQL connection pool created', {
      host: config.database.host,
      port: config.database.port,
      database: config.database.name,
    });
  }

  return pool;
}

/**
 * Executa uma query SELECT com parâmetros posicionais (?).
 * Retorna um array tipado de resultados.
 */
export async function query<T = Record<string, unknown>>(
  sql: string,
  params: (string | number | boolean | Date | null | undefined)[] = []
): Promise<T[]> {
  const start = Date.now();
  try {
    const [rows] = await getPool().execute<RowDataPacket[]>(sql, params as unknown as RowDataPacket[]);
    const duration = Date.now() - start;
    logger.debug('Database query executed', {
      query: sql.substring(0, 100),
      duration,
      rows: (rows as unknown[]).length,
    });
    return rows as unknown as T[];
  } catch (error) {
    logger.error('Database query error', {
      query: sql.substring(0, 100),
      error: (error as Error).message,
    });
    throw error;
  }
}

/**
 * Executa uma query de escrita (INSERT/UPDATE/DELETE).
 * Retorna o ResultSetHeader com insertId e affectedRows.
 */
export async function execute(
  sql: string,
  params: (string | number | boolean | Date | null | undefined)[] = []
): Promise<ResultSetHeader> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [result] = await getPool().execute<ResultSetHeader>(sql, params as any);
  return result;
}

/**
 * Executa múltiplas queries dentro de uma transação.
 */
export async function transaction<T>(
  fn: (conn: mysql.PoolConnection) => Promise<T>
): Promise<T> {
  const conn = await getPool().getConnection();
  await conn.beginTransaction();
  try {
    const result = await fn(conn);
    await conn.commit();
    return result;
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * Testa a conexão com o banco de dados
 */
export async function testConnection(): Promise<boolean> {
  try {
    const result = await query<{ now: string }>('SELECT NOW() AS now');
    logger.info('Database connection successful', { timestamp: result[0]?.now });
    return true;
  } catch (error) {
    logger.error('Database connection failed', { error: (error as Error).message });
    return false;
  }
}

/**
 * Fecha o pool de conexões
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    logger.info('Database pool closed');
  }
}
