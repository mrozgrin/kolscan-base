import winston from 'winston';

const { combine, timestamp, json, colorize, simple, errors } = winston.format;

const isDevelopment = process.env.NODE_ENV !== 'production';

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    errors({ stack: true }),
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    isDevelopment
      ? combine(colorize(), simple())
      : json()
  ),
  transports: [
    new winston.transports.Console(),
    ...(process.env.LOG_FILE
      ? [
          new winston.transports.File({
            filename: process.env.LOG_FILE,
            maxsize: 10 * 1024 * 1024, // 10MB
            maxFiles: 5,
          }),
        ]
      : []),
  ],
});

export default logger;
