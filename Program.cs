using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using NBomber.CSharp;

namespace Parkly.LoadTests
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            await Task.Delay(1);

            // --- الإعدادات الفعلية ---
            string signalrHubUrl = "https://parklysignalr.runasp.net/parkingHub";
            string hiveMqWebSocketUrl = "c335c915f7a540bcb9d83b6f4b0444f3.s1.eu.hivemq.cloud:8884/mqtt";
            string mqttUser = "esp32_worker";
            string mqttPass = "ParkMaster2026";

            var factory = new MqttFactory();

            // =========================================================
            // 1. سيناريو العدادات الوهمية (MQTT v4.3.7 Syntax)
            // =========================================================
            var hardwareScenario = Scenario.Create("hardware_army", async context =>
            {
                try
                {
                    using var mqttClient = factory.CreateMqttClient();

                    var mqttOptions = new MqttClientOptionsBuilder()
                        .WithClientId("Parkly_LoadTest_" + Guid.NewGuid())
                        .WithWebSocketServer(hiveMqWebSocketUrl) // متوافق مع v4
                        .WithCredentials(mqttUser, mqttPass)
                        .WithTls() // متوافق مع v4 وتختفي الـ CS0246
                        .Build();

                    await mqttClient.ConnectAsync(mqttOptions);

                    var randomDeviceNumber = Random.Shared.Next(1, 51);
                    var mockDto = new
                    {
                        Id = $"ESP32_Device_{randomDeviceNumber}",
                        RemA = Random.Shared.Next(0, 3600),
                        LocA = Random.Shared.Next(0, 2) == 1
                    };

                    string payloadJson = JsonSerializer.Serialize(mockDto);
                    string topic = $"city/street1/ESP32_Device_{randomDeviceNumber}/status";

                    // في إصدار 4، الـ WithPayload تقبل string أو byte[] مباشرة
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(topic)
                        .WithPayload(payloadJson)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build();

                    await mqttClient.PublishAsync(message);
                    await mqttClient.DisconnectAsync();

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    return Response.Fail(message: ex.Message, statusCode: "500");
                }
            })
            .WithLoadSimulations(
                Simulation.KeepConstant(copies: 10, during: TimeSpan.FromMinutes(1))
            );

            // =========================================================
            // 2. سيناريو اليوزرز الوهميين (SignalR)
            // =========================================================
            // =========================================================
            // 2. سيناريو اليوزرز العاديين (SignalR - Normal Users Load Test)
            // =========================================================
            var usersScenario = Scenario.Create("normal_users_army", async context =>
            {
                try
                {
                    await using var connection = new HubConnectionBuilder()
                        .WithUrl(signalrHubUrl)
                        .WithAutomaticReconnect()
                        .Build();

                    // السيرفر لما يبعت تحديث للعداد بيبعته على ميثود اسمها في الكلاينت (تأكد من اسمها عندك مثلاً "ReceiveMeterUpdate")
                    connection.On<object>("ReceiveMeterUpdate", (data) => {
                        // الكلاينت العادي بستقبل التحديثات هنا لايف
                    });

                    // 1. الاتصال بالـ SignalR Hub
                    await connection.StartAsync();

                    // محاكاة رقم عداد عشوائي بين 1 و 50 عشان نوزع الضغط على مصفات مختلفة
                    var randomMacId = $"ESP32_Device_{Random.Shared.Next(1, 51)}";
                    string meterType = "Standard"; // أو النوع المعتمد عندك في السيستم

                    // 2. استدعاء ميثود الـ Hub المخصصة لليوزر العادي عشان يسمع لتحديثات هاد العداد
                    await connection.InvokeAsync("SubscribeToMeter", randomMacId, meterType);

                    // 3. محاكاة يوزر حقيقي فاتح الأبليكيشن ووقّف يتطلع على الشاشة لمدة 7 ثوانٍ
                    // خلال ه الـ 7 ثوانٍ رح يقيس الـ NBomber كمية المسجات والضغط اللي بيتحملها السيرفر
                    await Task.Delay(7000);

                    // 4. اليوزر طلع من الأبليكيشن وسكر الاتصال
                    await connection.StopAsync();

                    return Response.Ok();
                }
                catch (Exception ex)
                {
                    return Response.Fail(message: ex.Message, statusCode: "500");
                }
            })
            .WithLoadSimulations(
                // محاكاة دخول 50 مستخدم عادي بنفس الوقت (بتقدر ترفع الرقم لـ 100 أو 200 لتجرب قوة الـ MonsterASP)
                Simulation.KeepConstant(copies: 50, during: TimeSpan.FromMinutes(1))
            );

            // =========================================================
            // 3. تشغيل الفحص
            // =========================================================
            Console.WriteLine("🚀 جاري بدء فحص الضغط لمشروع Parkly بالنسخة الموحدة 4.3.7...");

            NBomberRunner
                .RegisterScenarios(hardwareScenario, usersScenario)
                .Run();

            Console.WriteLine("✅ انتهى الفحص بنجاح!");
            Console.ReadLine();
        }
    }
}