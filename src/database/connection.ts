import { Pool, PoolClient } from 'pg';
import { config } from '../config';
import { logger } from '../utils/logger';

let pool: Pool | null = null;

export function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      host: config.database.host,
      port: config.database.port,
      database: config.database.name,
      user: config.database.user,
      password: config.database.password,
      ssl: config.database.ssl ? { rejectUnauthorized: false } : false,
      max: config.database.poolMax,
      min: config.database.poolMin,
      idleTimeoutMillis: config.database.poolIdleTimeout,
    });

    pool.on('error', (err) => {
      logger.error('Unexpected error on idle client', { error: err.message });
    });

    pool.on('connect', () => {
      logger.debug('New database connection established');
    });
  }

  return pool;
}

export async function query<T = unknown>(
  text: string,
  params?: unknown[]
): Promise<T[]> {
  const client = getPool();
  const start = Date.now();

  try {
    const result = await client.query(text, params);
    const duration = Date.now() - start;

    logger.debug('Database query executed', {
      query: text.substring(0, 100),
      duration,
      rows: result.rowCount,
    });

    return result.rows as T[];
  } catch (error) {
    logger.error('Database query error', {
      query: text.substring(0, 100),
      error: (error as Error).message,
    });
    throw error;
  }
}

export async function getClient(): Promise<PoolClient> {
  return getPool().connect();
}

export async function transaction<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await getClient();

  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function testConnection(): Promise<boolean> {
  try {
    const result = await query<{ now: Date }>('SELECT NOW()');
    logger.info('Database connection successful', { timestamp: result[0]?.now });
    return true;
  } catch (error) {
    logger.error('Database connection failed', { error: (error as Error).message });
    return false;
  }
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    logger.info('Database pool closed');
  }
}
