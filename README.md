# StayReady VPN

**English** · [Русский](#stayready-vpn-по-русски)

StayReady is a VPN for Windows, macOS, Linux, Android and Android TV for people who need a connection that simply works: at home, at work, on the road and on public Wi-Fi. Most VPN services rely on a single protocol. StayReady connects in several ways and restores the connection by itself when the network lets you down.

This repository holds the official StayReady downloads. Step-by-step guides and articles about VPN protocols are in the [wiki](https://github.com/stayready-app/releases/wiki). Website: [stayready.space](https://stayready.space).

<p align="center">
  <img src="https://stayready-app.github.io/site-media/help/windows/main-en.webp" height="200" alt="StayReady on a computer: connected, the location and the license on the right">
  <img src="https://stayready-app.github.io/site-media/help/android/main-en.webp" height="200" alt="StayReady on a phone: connected, the server chosen automatically">
  <img src="https://stayready-app.github.io/site-media/help/tv/app-main-en.webp" height="200" alt="StayReady on a TV: the menu on the left, the connect button in the middle, the server and the plan on the right">
</p>
<p align="center"><em>One app on a computer, a phone and a TV</em></p>

## Download

| System | File | Version |
|---|---|---|
| Windows 10 and 11, 64-bit | [StayReady-Setup-0.5.85-x64.exe](https://github.com/stayready-app/releases/releases/download/v0.5.85/StayReady-Setup-0.5.85-x64.exe) | 0.5.85 |
| Android 8.0 or later, phones and tablets (64-bit) | [StayReady-0.14.7-arm64.apk](https://github.com/stayready-app/releases/releases/download/android-v0.14.7/StayReady-0.14.7-arm64.apk) | 0.14.7 |
| Android TV, Google TV and other TVs on Android 8.0 or later | [StayReady-0.14.8-armv7.apk](https://github.com/stayready-app/releases/releases/download/android-v0.14.8/StayReady-0.14.8-armv7.apk) | 0.14.8 |
| macOS 12 Monterey or later, Apple silicon and Intel | [StayReady-0.1.2.dmg](https://github.com/stayready-app/releases/releases/download/macos-v0.1.2/StayReady-0.1.2.dmg) | 0.1.2 |
| Linux: Ubuntu 22.04 and Linux Mint 21 or later (.deb), Fedora (.rpm), 64-bit | [.deb](https://github.com/stayready-app/releases/releases/download/linux-v0.1.4/stayready_0.1.4_amd64.deb) · [.rpm](https://github.com/stayready-app/releases/releases/download/linux-v0.1.4/stayready-0.1.4-1.x86_64.rpm) | 0.1.4 |

Each file has a `.sha256` checksum on its [release page](https://github.com/stayready-app/releases/releases). A fresh installation starts on the FREE plan: no account, email or bank card is needed. How to install on each system, including a TV, is in the [guides](https://github.com/stayready-app/releases/wiki/Help).

## Why StayReady

- **Several protocols and transports.** VLESS + REALITY over HTTPS (TLS), VLESS over an SSH tunnel, a DNS tunnel or an RTSP tunnel, and our own protocol, StayReady Data Transport, over WebSocket or UDP. With **Automatic** the app picks the way and the server itself; on an unstable network you can choose them by hand. [More](https://github.com/stayready-app/releases/wiki/Why-StayReady)
- **Keeps working when the network does not.** The app checks the connection and restores it without manual reconfiguration. With automatic server selection it moves to another server if the current one stops answering.
- **Smart routing.** One ordered list of rules for sites, apps, addresses and UDP traffic. Ready rules open local banks, government services and the national domains of the country you are in directly, past the VPN, so you never switch it off to pay a bill.
- **Protection on public Wi-Fi.** In a café, hotel or airport the traffic between your device and the StayReady server is encrypted: nobody on the same network can read or alter it.
- **A free start without registration.** 1 GB a day for the first 7 days, then 256 MB and 90 minutes every day.
- **One account for all devices.** Sign in with a one-time code from your email, no password. Licenses, devices and purchases are in one place.

## Plans

| | FREE | BASIC | ADVANCED |
|---|---|---|---|
| Traffic | 1 GB a day for 7 days, then 256 MB and 90 minutes a day | Unlimited | Unlimited |
| Devices | 1 | Up to 2 | Up to 5 |
| Server | Chosen automatically | Chosen automatically | Your choice of server and country, additional servers, priority when servers are busy |
| Price | Free | $3.19 a month or $31.90 a year | $4.49 a month or $44.90 a year |

A plan is paid once for a month or for a year (a year costs the same as ten months); nothing is charged automatically. Current prices in every currency: [Pricing](https://github.com/stayready-app/releases/wiki/Pricing).

## Guides and articles

- **Guides:** [Windows](https://github.com/stayready-app/releases/wiki/Windows-guide) · [Android and Android TV](https://github.com/stayready-app/releases/wiki/Android-guide) · [macOS](https://github.com/stayready-app/releases/wiki/macOS-guide) · [Linux](https://github.com/stayready-app/releases/wiki/Linux-guide) · [Help and common questions](https://github.com/stayready-app/releases/wiki/Help)
- **VPN protocols:** [overview](https://github.com/stayready-app/releases/wiki/Protocols) · [VLESS](https://github.com/stayready-app/releases/wiki/VLESS) · [REALITY](https://github.com/stayready-app/releases/wiki/REALITY) · [XHTTP](https://github.com/stayready-app/releases/wiki/XHTTP) · [Xray and sing-box](https://github.com/stayready-app/releases/wiki/Xray) · [VLESS, WireGuard or OpenVPN](https://github.com/stayready-app/releases/wiki/VLESS-vs-WireGuard) · [SSH tunnel](https://github.com/stayready-app/releases/wiki/SSH-tunnel) · [DNS tunnel](https://github.com/stayready-app/releases/wiki/DNS-tunnel) · [RTSP tunnel](https://github.com/stayready-app/releases/wiki/RTSP-tunnel) · [StayReady Data Transport](https://github.com/stayready-app/releases/wiki/StayReady-Data-Transport)

## If stayready.space does not open

Everything you need to install is here: the files above and the guides in the wiki. To buy a plan or open your account, connect StayReady first (FREE works without an account) and then open the website.

## Support

Email [support@stayready.space](mailto:support@stayready.space) or use **Support** in the app.

---

# StayReady VPN по-русски

[English](#stayready-vpn) · **Русский**

StayReady — VPN для Windows, macOS, Linux, Android и Android TV для тех, кому нужно соединение, которое просто работает: дома, на работе, в дороге и в публичном Wi-Fi. Большинство VPN-сервисов полагаются на один протокол. StayReady умеет подключаться несколькими способами и сам восстанавливает соединение, когда сеть подводит.

В этом репозитории лежат официальные файлы для установки StayReady. Пошаговые инструкции и статьи о VPN-протоколах — в [вики](https://github.com/stayready-app/releases/wiki/%D0%93%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F). Сайт: [stayready.space](https://stayready.space/ru/).

<p align="center">
  <img src="https://stayready-app.github.io/site-media/help/windows/main-ru.webp" height="200" alt="StayReady на компьютере: подключено, справа расположение и лицензия">
  <img src="https://stayready-app.github.io/site-media/help/android/main-ru.webp" height="200" alt="StayReady на телефоне: подключено, сервер выбран автоматически">
  <img src="https://stayready-app.github.io/site-media/help/tv/app-main-ru.webp" height="200" alt="StayReady на телевизоре: слева меню, в центре кнопка подключения, справа сервер и тариф">
</p>
<p align="center"><em>Одно приложение на компьютере, телефоне и телевизоре</em></p>

## Скачать

| Система | Файл | Версия |
|---|---|---|
| Windows 10 и 11, 64-бит | [StayReady-Setup-0.5.85-x64.exe](https://github.com/stayready-app/releases/releases/download/v0.5.85/StayReady-Setup-0.5.85-x64.exe) | 0.5.85 |
| Android 8.0 и новее, телефоны и планшеты (64-бит) | [StayReady-0.14.7-arm64.apk](https://github.com/stayready-app/releases/releases/download/android-v0.14.7/StayReady-0.14.7-arm64.apk) | 0.14.7 |
| Android TV, Google TV и другие телевизоры на Android 8.0 и новее | [StayReady-0.14.8-armv7.apk](https://github.com/stayready-app/releases/releases/download/android-v0.14.8/StayReady-0.14.8-armv7.apk) | 0.14.8 |
| macOS 12 Monterey и новее, Apple silicon и Intel | [StayReady-0.1.2.dmg](https://github.com/stayready-app/releases/releases/download/macos-v0.1.2/StayReady-0.1.2.dmg) | 0.1.2 |
| Linux: Ubuntu 22.04 и Linux Mint 21 и новее (.deb), Fedora (.rpm), 64-бит | [.deb](https://github.com/stayready-app/releases/releases/download/linux-v0.1.4/stayready_0.1.4_amd64.deb) · [.rpm](https://github.com/stayready-app/releases/releases/download/linux-v0.1.4/stayready-0.1.4-1.x86_64.rpm) | 0.1.4 |

У каждого файла на [странице выпуска](https://github.com/stayready-app/releases/releases) есть контрольная сумма `.sha256`. Новая установка начинается с тарифа FREE: аккаунт, почта и банковская карта не нужны. Как установить приложение на каждую систему, в том числе на телевизор, — в [инструкциях](https://github.com/stayready-app/releases/wiki/%D0%9F%D0%BE%D0%BC%D0%BE%D1%89%D1%8C).

## Почему StayReady

- **Несколько протоколов и транспортов.** VLESS + REALITY через HTTPS (TLS), VLESS через SSH-туннель, DNS-туннель или RTSP-туннель и наш собственный протокол StayReady Data Transport через WebSocket или UDP. В режиме **Автоматически** приложение само выбирает способ и сервер; если сеть нестабильна, их можно выбрать вручную. [Подробнее](https://github.com/stayready-app/releases/wiki/%D0%9F%D0%BE%D1%87%D0%B5%D0%BC%D1%83-StayReady)
- **Работает, когда сеть подводит.** Приложение само проверяет соединение и восстанавливает его без ручной перенастройки. При автоматическом выборе сервера оно переходит на другой сервер, если текущий перестал отвечать.
- **Умная маршрутизация.** Один упорядоченный список правил для сайтов, приложений, адресов и UDP-трафика. Готовые правила открывают местные банки, госуслуги и национальные домены страны, в которой вы находитесь, напрямую, мимо VPN, — не нужно выключать VPN, чтобы оплатить счёт.
- **Защита в публичном Wi-Fi.** В кафе, гостинице или аэропорту трафик между устройством и сервером StayReady зашифрован: никто в той же сети не может его прочитать или изменить.
- **Бесплатный старт без регистрации.** Первые 7 дней — 1 ГБ в сутки, затем 256 МБ и 90 минут каждый день.
- **Один аккаунт для всех устройств.** Вход по одноразовому коду из письма, без пароля. Лицензии, устройства и покупки — в одном месте.

## Тарифы

| | FREE | BASIC | ADVANCED |
|---|---|---|---|
| Трафик | 7 дней по 1 ГБ в сутки, затем 256 МБ и 90 минут в сутки | Без ограничений | Без ограничений |
| Устройства | 1 | До 2 | До 5 |
| Сервер | Выбирается автоматически | Выбирается автоматически | Выбор сервера и страны, дополнительные серверы, приоритет при нагрузке |
| Цена | Бесплатно | 249 ₽ в месяц или 2 490 ₽ в год | 349 ₽ в месяц или 3 490 ₽ в год |

Тариф оплачивается один раз на месяц или на год (год стоит как десять месяцев); ничего не списывается автоматически. Актуальные цены во всех валютах: [Тарифы](https://github.com/stayready-app/releases/wiki/%D0%A2%D0%B0%D1%80%D0%B8%D1%84%D1%8B).

## Инструкции и статьи

- **Инструкции:** [Windows](https://github.com/stayready-app/releases/wiki/%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%86%D0%B8%D1%8F-%D0%B4%D0%BB%D1%8F-Windows) · [Android и Android TV](https://github.com/stayready-app/releases/wiki/%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%86%D0%B8%D1%8F-%D0%B4%D0%BB%D1%8F-Android) · [macOS](https://github.com/stayready-app/releases/wiki/%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%86%D0%B8%D1%8F-%D0%B4%D0%BB%D1%8F-macOS) · [Linux](https://github.com/stayready-app/releases/wiki/%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%86%D0%B8%D1%8F-%D0%B4%D0%BB%D1%8F-Linux) · [Помощь и частые вопросы](https://github.com/stayready-app/releases/wiki/%D0%9F%D0%BE%D0%BC%D0%BE%D1%89%D1%8C)
- **VPN-протоколы:** [обзор](https://github.com/stayready-app/releases/wiki/%D0%9F%D1%80%D0%BE%D1%82%D0%BE%D0%BA%D0%BE%D0%BB%D1%8B) · [VLESS](https://github.com/stayready-app/releases/wiki/%D0%9F%D1%80%D0%BE%D1%82%D0%BE%D0%BA%D0%BE%D0%BB-VLESS) · [REALITY](https://github.com/stayready-app/releases/wiki/%D0%9F%D1%80%D0%BE%D1%82%D0%BE%D0%BA%D0%BE%D0%BB-REALITY) · [XHTTP](https://github.com/stayready-app/releases/wiki/%D0%A2%D1%80%D0%B0%D0%BD%D1%81%D0%BF%D0%BE%D1%80%D1%82-XHTTP) · [Xray и sing-box](https://github.com/stayready-app/releases/wiki/%D0%AF%D0%B4%D1%80%D0%BE-Xray) · [VLESS, WireGuard или OpenVPN](https://github.com/stayready-app/releases/wiki/VLESS-%D0%B8%D0%BB%D0%B8-WireGuard) · [SSH-туннель](https://github.com/stayready-app/releases/wiki/SSH-%D1%82%D1%83%D0%BD%D0%BD%D0%B5%D0%BB%D1%8C) · [DNS-туннель](https://github.com/stayready-app/releases/wiki/DNS-%D1%82%D1%83%D0%BD%D0%BD%D0%B5%D0%BB%D1%8C) · [RTSP-туннель](https://github.com/stayready-app/releases/wiki/RTSP-%D1%82%D1%83%D0%BD%D0%BD%D0%B5%D0%BB%D1%8C) · [StayReady Data Transport](https://github.com/stayready-app/releases/wiki/%D0%9F%D1%80%D0%BE%D1%82%D0%BE%D0%BA%D0%BE%D0%BB-SRDT)

## Если stayready.space не открывается

Всё, что нужно для установки, есть здесь: файлы выше и инструкции в вики. Чтобы купить тариф или открыть личный кабинет, сначала подключите StayReady (FREE работает без аккаунта), а затем откройте сайт.

## Поддержка

Пишите на [support@stayready.space](mailto:support@stayready.space) или через раздел **Поддержка** в приложении.

---

© 2026 StayReady TOO
