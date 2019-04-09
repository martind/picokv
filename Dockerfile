FROM node
WORKDIR /app
COPY package*.json .
RUN yarn install
COPY server.js picokv.js test.sh ./
EXPOSE 9001
CMD ["node", "server.js"]

