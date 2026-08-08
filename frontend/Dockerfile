FROM node:22-alpine AS build

WORKDIR /app

ARG VITE_MONITOR_API_BASE_URL=auto
ENV VITE_MONITOR_API_BASE_URL=${VITE_MONITOR_API_BASE_URL}

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM node:22-alpine AS serve

WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

EXPOSE 5173
CMD ["npm", "run", "serve"]
