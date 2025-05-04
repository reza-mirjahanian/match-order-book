FROM node:23-alpine AS base

FROM base AS development


ARG NODE_ENV=development
ENV NODE_ENV=${NODE_ENV}

WORKDIR /usr/src/app
COPY package.json package-lock.json ./
RUN npm install
COPY . .


RUN npm run build




RUN chmod +x ./start.sh

CMD ["/bin/sh","start.sh"]

FROM development AS testing

ENV NODE_ENV=test
RUN chmod +x ./start.test.sh

CMD ["/bin/sh","start.test.sh"]