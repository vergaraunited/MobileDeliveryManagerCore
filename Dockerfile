# FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build
FROM microsoft/dotnet:2.1-runtime AS base
WORKDIR /app

FROM microsoft/dotnet:2.1-sdk AS build
ARG buildno
ARG gitcommithash

RUN echo "Build number: $buildno"
RUN echo "Based on commit: $gitcommithash"
WORKDIR /src
COPY MobileDeliveryManagerCore.csproj .
COPY nuget.config .
RUN dir
RUN dotnet restore
COPY . .

FROM build AS publish
RUN dotnet publish -c Release -o /app

FROM base AS final
WORKDIR /app
RUN dir 
COPY --from=publish /app .
COPY /logs .
RUN dir
EXPOSE 81
EXPOSE 1433
EXPOSE 8181
ENTRYPOINT ["dotnet", "MobileDeliveryManager.dll"]

