import discord
from discord import app_commands, ui, ButtonStyle, SelectOption, Interaction, Embed, Color
from discord.ext import commands
import aiohttp
import asyncio
import motor.motor_asyncio
import datetime
import os
import traceback
from typing import Optional, Dict, List, Any
from bson import ObjectId

# ===================== CONFIGURACIÓN =====================
class Config:
    TOKEN = os.getenv("TOKEN", "TU_TOKEN_AQUI")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    OWNER_ID = 1452608365912920170

    # Roles
    STAFF_GENERAL = 1467132351409819668
    PROPIETARIO = 1452608365912920170
    DUENO = 1482020630688956508
    CO_DUENO = 1500573270112473211
    FUNDADOR = 1500573616708911386
    CO_FUNDADOR = 1452608365912920172
    DIRECTOR_GENERAL = 1520374435536961556
    MANAGER = 1457083463424278741
    SUPERVISOR = 1509198197900448037
    COORDINADOR = 1489591346804166757
    DIRECTOR_ADMINISTRATIVO = 1520375021220925630
    ADMINISTRADOR_JEFE = 1452608365892206641
    ADMINISTRADOR = 1452608365892206640
    DIRECTOR_MODERATIVO = 1489584063667896321
    MODERADOR = 1480324121551437944
    AYUDANTE = 1452608365812515000
    SOPORTE = 1452608365862584423
    HELPER = 1492915595249975307
    VERIFICADO = 1452608365795610694
    NO_VERIFICADO = 1452608365795610692

    # Categorías de tickets
    CAT_TICKETS_GENERAL = 1480580609075052757
    CAT_TICKETS_FUNDACION = 1510944914718982254
    CAT_TICKETS_FACCIONES_LEGALES = 1510944986248646666
    CAT_TICKETS_FACCIONES_ILEGALES = 1510945054808735798
    CAT_TICKETS_REPORTES = 1510945156159766598
    CAT_TICKETS_EMPRESAS = 1510945204067369121
    CAT_TICKETS_DONACION = 1510945244605190184
    CAT_TICKETS_INCIDENCIAS_TECNICAS = 1510945373143699457
    CAT_TICKETS_VERIFICACION = 1516371570921046088

    TICKET_CATEGORIES = [
        CAT_TICKETS_GENERAL,
        CAT_TICKETS_FUNDACION,
        CAT_TICKETS_FACCIONES_LEGALES,
        CAT_TICKETS_FACCIONES_ILEGALES,
        CAT_TICKETS_REPORTES,
        CAT_TICKETS_EMPRESAS,
        CAT_TICKETS_DONACION,
        CAT_TICKETS_INCIDENCIAS_TECNICAS,
        CAT_TICKETS_VERIFICACION,
    ]

    # Canales
    CH_BIENVENIDAS = 1452608366860959826
    CH_DESPEDIDAS = 1452608366860959827
    CH_REVISIONES_VERIFICACION = 1452668749701189632
    CH_SANCIONES_PUBLICAS = 1452608367817261069
    CH_SANCIONES_STAFF = 1453030061199589591
    CH_TICKETS_PRINCIPAL = 1452632044470538443
    CH_VALORACION_STAFF = 1452608367817261068
    CH_CITACIONES = 1495441610656448693
    CH_VERIFICACION_PANEL = 1505926635239768074

    # Logs
    LOG_GENERAL = 1489589357580128436
    LOG_MODERACION = 1489589289778941962
    LOG_EMOJIS = 1489589189954506772
    LOG_ROLES = 1489589108786331799
    LOG_MIEMBROS = 1489589153485029536
    LOG_CANALES = 1489589238335803422
    LOG_INVITACIONES = 1489589554032676884
    LOG_MENSAJES = 1489588046910459904
    LOG_TICKETS = 1489661308931674292
    LOG_VALORACIONES = 1489589357580128436

    VERIFICATION_TIMEOUT = 1800
    SERVER_LOGO = "https://i.imgur.com/placeholder.png"

# ===================== BASE DE DATOS (MongoDB) =====================
class Database:
    def __init__(self, mongo_uri: str):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.client["cadiz_rp"]
        self.sanciones = self.db["sanciones"]
        self.server_state = self.db["server_state"]
        self.votacion = self.db["votacion"]
        self.valoraciones = self.db["valoraciones"]

    async def init(self):
        if not await self.server_state.find_one({"key": "status"}):
            await self.server_state.insert_one({"key": "status", "value": "cerrado"})
        if not await self.votacion.find_one({"key": "votacion"}):
            await self.votacion.insert_one({"key": "votacion", "timestamp": 0})

    async def add_sancion(self, usuario_id, usuario_nombre, staff_id, staff_nombre, motivo, tipo, pruebas, observaciones):
        doc = {
            "usuario_id": usuario_id,
            "usuario_nombre": usuario_nombre,
            "staff_id": staff_id,
            "staff_nombre": staff_nombre,
            "motivo": motivo,
            "tipo": tipo,
            "pruebas": pruebas,
            "observaciones": observaciones,
            "fecha": datetime.datetime.utcnow()
        }
        result = await self.sanciones.insert_one(doc)
        return str(result.inserted_id)

    async def get_sanciones_usuario(self, usuario_id):
        cursor = self.sanciones.find({"usuario_id": usuario_id}).sort("fecha", -1).limit(10)
        return await cursor.to_list(length=10)

    async def get_sanciones_staff_emitidas(self, staff_id):
        cursor = self.sanciones.find({"staff_id": staff_id}).sort("fecha", -1).limit(10)
        return await cursor.to_list(length=10)

    async def eliminar_sancion(self, sancion_id):
        try:
            result = await self.sanciones.delete_one({"_id": ObjectId(sancion_id)})
            return result.deleted_count > 0
        except:
            return False

    async def get_all_sanciones(self):
        cursor = self.sanciones.find().sort("fecha", -1)
        return await cursor.to_list(length=None)

    async def get_server_status(self):
        doc = await self.server_state.find_one({"key": "status"})
        return doc["value"] if doc else "cerrado"

    async def set_server_status(self, status):
        await self.server_state.update_one({"key": "status"}, {"$set": {"value": status}}, upsert=True)

    async def get_votacion_timestamp(self):
        doc = await self.votacion.find_one({"key": "votacion"})
        return doc["timestamp"] if doc else 0

    async def set_votacion_timestamp(self, timestamp):
        await self.votacion.update_one({"key": "votacion"}, {"$set": {"timestamp": timestamp}}, upsert=True)

    async def add_valoracion(self, user_id, user_name, staff_id, staff_name, puntuacion, comentario, ticket_id=None):
        doc = {
            "user_id": user_id,
            "user_name": user_name,
            "staff_id": staff_id,
            "staff_name": staff_name,
            "puntuacion": puntuacion,
            "comentario": comentario,
            "ticket_id": ticket_id,
            "fecha": datetime.datetime.utcnow()
        }
        result = await self.valoraciones.insert_one(doc)
        return str(result.inserted_id)

    async def get_valoraciones_usuario(self, user_id):
        cursor = self.valoraciones.find({"user_id": user_id}).sort("fecha", -1)
        return await cursor.to_list(length=None)

    async def get_valoraciones_staff(self, staff_id):
        cursor = self.valoraciones.find({"staff_id": staff_id}).sort("fecha", -1)
        return await cursor.to_list(length=None)

    async def get_promedio_staff(self, staff_id):
        cursor = self.valoraciones.find({"staff_id": staff_id})
        valoraciones = await cursor.to_list(length=None)
        if not valoraciones:
            return None
        total = sum(v["puntuacion"] for v in valoraciones)
        return round(total / len(valoraciones), 2)

# ===================== BOT =====================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.emojis = True
intents.bans = True
intents.invites = True
intents.guilds = True
intents.moderation = True
intents.reactions = True
intents.voice_states = True
bot = commands.Bot(command_prefix="!", intents=intents)

db = Database(Config.MONGO_URI)
verification_sessions: Dict[int, dict] = {}
ticket_data: Dict[int, dict] = {}

# ===================== LOGGER =====================
class Logger:
    @staticmethod
    async def send_log(guild: discord.Guild, channel_id: int, embed: Embed):
        channel = guild.get_channel(channel_id)
        if channel:
            try:
                await channel.send(embed=embed)
            except:
                pass

    @staticmethod
    def create_base_embed(title: str, description: str, color: Color, author: Optional[discord.Member] = None, target: Optional[str] = None) -> Embed:
        embed = Embed(title=title, description=description, color=color, timestamp=datetime.datetime.utcnow())
        embed.set_footer(text=get_footer())
        embed.add_field(name="📅 Fecha/Hora (UTC)", value=datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S"), inline=False)
        if author:
            embed.add_field(name="👤 Responsable", value=f"{author.mention} ({author.id})", inline=False)
        if target:
            embed.add_field(name="🎯 Sobre", value=target, inline=False)
        return embed

    @staticmethod
    async def log_general(guild: discord.Guild, title: str, description: str, color: Color = Color.blue(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_GENERAL, embed)

    @staticmethod
    async def log_moderacion(guild: discord.Guild, title: str, description: str, color: Color = Color.red(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_MODERACION, embed)

    @staticmethod
    async def log_emojis(guild: discord.Guild, title: str, description: str, color: Color = Color.gold(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_EMOJIS, embed)

    @staticmethod
    async def log_roles(guild: discord.Guild, title: str, description: str, color: Color = Color.purple(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_ROLES, embed)

    @staticmethod
    async def log_miembros(guild: discord.Guild, title: str, description: str, color: Color = Color.green(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_MIEMBROS, embed)

    @staticmethod
    async def log_canales(guild: discord.Guild, title: str, description: str, color: Color = Color.teal(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_CANALES, embed)

    @staticmethod
    async def log_invitaciones(guild: discord.Guild, title: str, description: str, color: Color = Color.dark_teal(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_INVITACIONES, embed)

    @staticmethod
    async def log_mensajes(guild: discord.Guild, title: str, description: str, color: Color = Color.orange(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_MENSAJES, embed)

    @staticmethod
    async def log_tickets(guild: discord.Guild, title: str, description: str, color: Color = Color.blurple(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_TICKETS, embed)

    @staticmethod
    async def log_valoracion(guild: discord.Guild, title: str, description: str, color: Color = Color.gold(), author: discord.Member = None, target: str = None):
        embed = Logger.create_base_embed(title, description, color, author, target)
        await Logger.send_log(guild, Config.LOG_VALORACIONES, embed)

    @staticmethod
    async def archive_ticket(channel: discord.TextChannel, closer: discord.Member):
        guild = channel.guild
        messages = []
        async for msg in channel.history(limit=2000, oldest_first=True):
            attachments = [a.url for a in msg.attachments]
            attachments_str = ", ".join(attachments) if attachments else "Ninguno"
            messages.append(
                f"**{msg.author}** ({msg.author.id}) | {msg.created_at.strftime('%d/%m/%Y %H:%M:%S')}\n"
                f"{msg.content if msg.content else '(Sin contenido)'}\n"
                f"📎 Adjuntos: {attachments_str}\n"
                f"---"
            )

        transcript = "\n".join(messages)
        if len(transcript) > 4000:
            transcript = transcript[:4000] + "\n... (truncado)"
        embed = Embed(
            title=f"📄 Transcripción de {channel.name}",
            description=f"**Canal:** {channel.mention}\n**Categoría:** {channel.category.name if channel.category else 'Sin categoría'}\n"
                        f"**Cerrado por:** {closer.mention} ({closer.id})\n"
                        f"**Total de mensajes:** {len(messages)}",
            color=Color.blurple(),
            timestamp=datetime.datetime.utcnow()
        )
        embed.add_field(name="📅 Fecha/Hora (UTC)", value=datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S"), inline=False)
        embed.add_field(name="👤 Cerrado por", value=f"{closer.mention} ({closer.id})", inline=False)
        embed.add_field(name="📝 Transcripción", value=transcript, inline=False)
        embed.set_footer(text=get_footer())
        await Logger.send_log(guild, Config.LOG_TICKETS, embed)

# ===================== UTILIDADES =====================
def is_staff(member: discord.Member) -> bool:
    if not member.guild:
        return False
    staff_role = member.guild.get_role(Config.STAFF_GENERAL)
    return staff_role in member.roles

def is_fundador_or_higher(member: discord.Member) -> bool:
    roles_ids = [Config.FUNDADOR, Config.CO_FUNDADOR, Config.CO_DUENO, Config.DUENO, Config.PROPIETARIO]
    member_roles = [r.id for r in member.roles]
    return any(r_id in member_roles for r_id in roles_ids)

def is_owner_or_dueno(member: discord.Member) -> bool:
    if member.id == Config.OWNER_ID:
        return True
    roles_ids = [Config.PROPIETARIO, Config.DUENO]
    member_roles = [r.id for r in member.roles]
    return any(r_id in member_roles for r_id in roles_ids)

def is_ticket_channel(channel: discord.TextChannel) -> bool:
    if not channel.category:
        return False
    return channel.category.id in Config.TICKET_CATEGORIES

def get_utc_timestamp():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def get_footer():
    return f"© Cádiz RP • {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}"

async def send_dm(user: discord.User, content: str = "", embed: Embed = None, view: ui.View = None) -> bool:
    try:
        if view:
            await user.send(content, embed=embed, view=view)
        elif embed:
            await user.send(content, embed=embed)
        else:
            await user.send(content)
        return True
    except discord.Forbidden:
        return False

async def create_ticket_channel(guild: discord.Guild, category_id: int, user: discord.Member,
                                channel_name: str, topic: str) -> discord.TextChannel:
    category = guild.get_channel(category_id)
    if not category:
        raise ValueError("Categoría no encontrada")

    if await has_active_ticket(user):
        raise ValueError("Ya tienes un ticket abierto. Cierra el actual antes de abrir otro.")

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    staff_role = guild.get_role(Config.STAFF_GENERAL)
    if staff_role:
        overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    channel = await guild.create_text_channel(
        name=channel_name,
        category=category,
        overwrites=overwrites,
        topic=topic
    )
    ticket_data[channel.id] = {"owner_id": user.id, "claimed_by": None, "locked": False, "closed": False}
    return channel

async def has_active_ticket(user: discord.Member) -> bool:
    guild = user.guild
    for category_id in Config.TICKET_CATEGORIES:
        category = guild.get_channel(category_id)
        if not category:
            continue
        for channel in category.channels:
            if isinstance(channel, discord.TextChannel):
                perms = channel.permissions_for(user)
                if perms.read_messages and perms.send_messages:
                    data = ticket_data.get(channel.id)
                    if data and not data.get("closed", False):
                        return True
    return False

async def get_audit_log_moderator(guild: discord.Guild, action: discord.AuditLogAction, target_id: int) -> Optional[discord.Member]:
    try:
        async for entry in guild.audit_logs(action=action, limit=10):
            if entry.target.id == target_id:
                return entry.user
    except:
        pass
    return None

# ===================== MODAL DE VALORACIÓN =====================
class ValorarModal(ui.Modal, title="Valoración del staff"):
    puntuacion = ui.TextInput(
        label="Puntuación (1-10)",
        placeholder="Ej: 8",
        min_length=1,
        max_length=2,
        required=True
    )
    comentario = ui.TextInput(
        label="Comentario (opcional)",
        placeholder="¿Qué tal fue la atención?",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=500
    )

    def __init__(self, user: discord.Member, staff_id: int, ticket_id: int):
        super().__init__()
        self.user = user
        self.staff_id = staff_id
        self.ticket_id = ticket_id

    async def on_submit(self, interaction: Interaction):
        try:
            punt = int(self.puntuacion.value)
            if punt < 1 or punt > 10:
                await interaction.response.send_message("❌ La puntuación debe ser un número entre 1 y 10.", ephemeral=True)
                return
        except ValueError:
            await interaction.response.send_message("❌ Debes introducir un número válido entre 1 y 10.", ephemeral=True)
            return

        staff_member = interaction.guild.get_member(self.staff_id)
        staff_name = staff_member.display_name if staff_member else f"Staff ID {self.staff_id}"

        await db.add_valoracion(
            user_id=self.user.id,
            user_name=self.user.display_name,
            staff_id=self.staff_id,
            staff_name=staff_name,
            puntuacion=punt,
            comentario=self.comentario.value,
            ticket_id=str(self.ticket_id)
        )

        embed_resp = Embed(
            title="✅ ¡Gracias por valorar!",
            description=f"Has valorado al staff con **{punt}/10**.\n"
                        f"Comentario: {self.comentario.value if self.comentario.value else 'Sin comentario'}",
            color=Color.green()
        )
        embed_resp.set_footer(text=get_footer())
        await interaction.response.send_message(embed=embed_resp, ephemeral=True)

        embed_log = Embed(
            title="⭐ Nueva valoración de staff",
            description=f"**Usuario:** {self.user.mention} ({self.user.id})\n"
                        f"**Staff valorado:** {staff_member.mention if staff_member else staff_name} ({self.staff_id})\n"
                        f"**Puntuación:** {punt}/10\n"
                        f"**Comentario:** {self.comentario.value if self.comentario.value else 'Sin comentario'}\n"
                        f"**Ticket ID:** {self.ticket_id}",
            color=Color.gold(),
            timestamp=datetime.datetime.utcnow()
        )
        embed_log.set_footer(text=get_footer())
        await Logger.send_log(interaction.guild, Config.LOG_VALORACIONES, embed_log)

        if staff_member:
            embed_dm = Embed(
                title="⭐ Has recibido una valoración",
                description=f"**Usuario:** {self.user.mention} ({self.user.id})\n"
                            f"**Puntuación:** {punt}/10\n"
                            f"**Comentario:** {self.comentario.value if self.comentario.value else 'Sin comentario'}",
                color=Color.gold()
            )
            embed_dm.set_footer(text=get_footer())
            await send_dm(staff_member, embed=embed_dm)

# ===================== VISTA DE VALORACIÓN =====================
class ValorarView(ui.View):
    def __init__(self, user: discord.Member, staff_id: int, ticket_id: int):
        super().__init__(timeout=300)
        self.user = user
        self.staff_id = staff_id
        self.ticket_id = ticket_id
        self.valorado = False

    @ui.button(label="⭐ Valorar atención", style=ButtonStyle.primary, custom_id="valorar_staff")
    async def valorar_button(self, interaction: Interaction, button: ui.Button):
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("Este mensaje no es para ti.", ephemeral=True)
            return
        if self.valorado:
            await interaction.response.send_message("Ya has valorado este ticket.", ephemeral=True)
            return
        modal = ValorarModal(self.user, self.staff_id, self.ticket_id)
        await interaction.response.send_modal(modal)
        self.valorado = True
        self.children[0].disabled = True
        await interaction.message.edit(view=self)

# ===================== EVALUADOR DE VERIFICACIÓN =====================
class VerificationEvaluator:
    @staticmethod
    def evaluate_mg(answer):
        score = 0
        alerts = []
        if "metagaming" in answer.lower() or "información" in answer.lower() or "fuera de rol" in answer.lower():
            score += 10
        else:
            alerts.append("Definición de MG poco clara")
        if "ejemplo" in answer.lower() or "ej:" in answer.lower():
            score += 10
        else:
            alerts.append("Falta ejemplo concreto de MG")
        return score, alerts

    @staticmethod
    def evaluate_pg(answer):
        score = 0
        alerts = []
        if "powergaming" in answer.lower() or "forzar" in answer.lower() or "acción imposible" in answer.lower():
            score += 10
        else:
            alerts.append("Definición de PG poco clara")
        if "ejemplo" in answer.lower() or "ej:" in answer.lower():
            score += 10
        else:
            alerts.append("Falta ejemplo concreto de PG")
        return score, alerts

    @staticmethod
    def evaluate_failrp(answer):
        score = 0
        alerts = []
        if "reportar" in answer.lower() or "antirol" in answer.lower() or "romper" in answer.lower():
            score += 8
        else:
            alerts.append("No menciona reportar adecuadamente")
        if "seguir" in answer.lower() and "rol" in answer.lower():
            score += 7
        else:
            alerts.append("No menciona continuar el rol sin romperlo")
        return score, alerts

    @staticmethod
    def evaluate_situation(answer):
        score = 0
        alerts = []
        if "reportar" in answer.lower() or "admin" in answer.lower() or "moderador" in answer.lower():
            score += 8
        else:
            alerts.append("No menciona reportar la situación")
        if "pruebas" in answer.lower() or "grabación" in answer.lower() or "captura" in answer.lower():
            score += 7
        else:
            alerts.append("No menciona recopilar pruebas")
        return score, alerts

    @staticmethod
    def evaluate_compromiso(nivel_roleo, como_metiste):
        score = 0
        if nivel_roleo.isdigit() and int(nivel_roleo) >= 5:
            score += 5
        if "anterior" in como_metiste.lower() or "invitación" in como_metiste.lower():
            score += 5
        elif "búsqueda" in como_metiste.lower() or "redes" in como_metiste.lower():
            score += 3
        return score

    @staticmethod
    def evaluate_bonus(answers):
        bonus = 0
        for key, value in answers.items():
            if isinstance(value, str) and len(value.split()) > 10:
                bonus += 2
        if bonus > 15:
            bonus = 15
        return bonus

    @classmethod
    def evaluate_all(cls, answers):
        mg_score, mg_alerts = cls.evaluate_mg(answers.get('mg', ''))
        pg_score, pg_alerts = cls.evaluate_pg(answers.get('pg', ''))
        failrp_score, failrp_alerts = cls.evaluate_failrp(answers.get('antirol', ''))
        situation_score, situation_alerts = cls.evaluate_situation(answers.get('antirol', ''))
        compromiso_score = cls.evaluate_compromiso(answers.get('nivel_roleo', '0'), answers.get('como_metiste', ''))
        bonus_score = cls.evaluate_bonus(answers)

        total = mg_score + pg_score + failrp_score + situation_score + compromiso_score + bonus_score

        alerts = []
        alerts.extend(mg_alerts)
        alerts.extend(pg_alerts)
        alerts.extend(failrp_alerts)
        alerts.extend(situation_alerts)
        if compromiso_score < 5:
            alerts.append("Compromiso bajo con el rol")
        if bonus_score < 5:
            alerts.append("Respuestas demasiado cortas o poco detalladas")

        if total >= 85:
            decision = "APROBAR"
            motivo = "Excelente conocimiento y compromiso. Aprobado automáticamente."
        elif total >= 60:
            decision = "REVISAR"
            motivo = "Conocimiento básico aceptable pero con algunas respuestas que merecen revisión manual."
        else:
            decision = "RECHAZAR"
            motivo = "Puntuación baja. Se recomienda rechazar o pedir una nueva solicitud."

        return {
            "mg": mg_score,
            "pg": pg_score,
            "failrp": failrp_score,
            "situation": situation_score,
            "compromiso": compromiso_score,
            "bonus": bonus_score,
            "total": total,
            "alerts": alerts,
            "decision": decision,
            "motivo": motivo
        }

# ===================== VIEWS =====================
class ConfirmView(ui.View):
    def __init__(self, user_id: int, interaction: Interaction):
        super().__init__(timeout=60.0)
        self.user_id = user_id
        self.interaction = interaction
        self.confirmed = False

    @ui.button(label="✅ Sí, estoy seguro", style=ButtonStyle.success, custom_id="confirm_yes")
    async def confirm_yes(self, interaction: Interaction, button: ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Este mensaje no es para ti.", ephemeral=True)
            return
        self.confirmed = True
        await interaction.response.send_message("✅ ¡Perfecto! Comenzamos con la verificación.", ephemeral=True)

        user = interaction.user
        embed = Embed(
            title="🔐 Verificación - Cádiz RP",
            description="Responde a las preguntas que te haré por este canal. Tienes **30 minutos** para completar el proceso.\n\n**Pregunta 1:** ¿Cuál es tu usuario de Roblox?",
            color=Color.blue()
        )
        embed.set_footer(text=get_footer())

        try:
            await user.send(embed=embed)
            verification_sessions[user.id] = {
                "step": 1,
                "answers": {},
                "timeout_task": asyncio.create_task(self.timeout_verification(user))
            }
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ No puedo enviarte mensajes directos. Por favor, habilita los DMs de este servidor o abre un ticket.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Ocurrió un error al iniciar la verificación. Por favor, intenta de nuevo o abre un ticket.",
                ephemeral=True
            )
        self.stop()

    @ui.button(label="❌ No, cancelar", style=ButtonStyle.danger, custom_id="confirm_no")
    async def confirm_no(self, interaction: Interaction, button: ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("Este mensaje no es para ti.", ephemeral=True)
            return
        self.confirmed = False
        await interaction.response.send_message("❌ Proceso cancelado. Si cambias de opinión, vuelve a presionar **COMENZAR**.", ephemeral=True)
        self.stop()

    async def on_timeout(self):
        if not self.confirmed:
            try:
                user = await bot.fetch_user(self.user_id)
                await user.send("⏰ Tiempo agotado. No has confirmado a tiempo. Vuelve a intentarlo desde el panel.")
            except:
                pass
        self.stop()

    async def timeout_verification(self, user: discord.User):
        await asyncio.sleep(Config.VERIFICATION_TIMEOUT)
        if user.id in verification_sessions:
            session = verification_sessions.pop(user.id, None)
            if session and session.get("timeout_task"):
                try:
                    embed = Embed(
                        title="⏰ Tiempo agotado",
                        description="El proceso de verificación ha expirado. Por favor, inicia uno nuevo desde el panel.",
                        color=Color.red()
                    )
                    embed.set_footer(text=get_footer())
                    await user.send(embed=embed)
                except:
                    pass

class VerificationPanelView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @ui.button(label="▶️ COMENZAR", style=ButtonStyle.success, custom_id="verify_start")
    async def start_verification(self, interaction: Interaction, button: ui.Button):
        user = interaction.user
        if user.id in verification_sessions:
            await interaction.response.send_message(
                "Ya tienes un proceso de verificación en curso. Revisa tus mensajes directos.",
                ephemeral=True
            )
            return

        try:
            await user.send("🔍 Comprobando permisos de DM...")
            async for msg in user.history(limit=1):
                if msg.content == "🔍 Comprobando permisos de DM...":
                    await msg.delete()
                    break
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ No puedo enviarte mensajes directos. Por favor, habilita los DMs de este servidor y vuelve a intentarlo.",
                ephemeral=True
            )
            return
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error al verificar permisos. Por favor, intenta de nuevo.",
                ephemeral=True
            )
            return

        embed_confirm = Embed(
            title="⚠️ Confirmación necesaria",
            description="Has solicitado iniciar el proceso de verificación en **Cádiz RP**.\n\n"
                        "Este proceso te hará 8 preguntas y tomará unos minutos.\n"
                        "¿Estás seguro de que deseas continuar?\n\n"
                        "Tienes **60 segundos** para decidir.",
            color=Color.orange()
        )
        embed_confirm.set_footer(text=get_footer())
        try:
            view = ConfirmView(user.id, interaction)
            await user.send(embed=embed_confirm, view=view)
            await interaction.response.send_message(
                "✅ Te he enviado un mensaje directo con la confirmación. **Revisa tus DMs** y pulsa 'Sí, estoy seguro' para comenzar.",
                ephemeral=True
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ No puedo enviarte mensajes directos. Por favor, habilita los DMs de este servidor.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error al enviar el mensaje de confirmación. Por favor, intenta de nuevo.",
                ephemeral=True
            )

    @ui.button(label="🎫 TICKET", style=ButtonStyle.primary, custom_id="verify_ticket")
    async def ticket_verification(self, interaction: Interaction, button: ui.Button):
        user = interaction.user
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
            return
        try:
            channel_name = f"ticket-verificacion-{user.name[:10]}"
            topic = f"Ticket abierto por {user.mention} a las {datetime.datetime.utcnow().strftime('%H:%M UTC')} en la categoría Verificación"
            channel = await create_ticket_channel(guild, Config.CAT_TICKETS_VERIFICACION, user, channel_name, topic)
            embed = Embed(
                title="🎫 Ticket de Verificación",
                description=f"**Usuario:** {user.mention}\n**Tipo:** Verificación\n\n"
                            "Un miembro del staff se pondrá en contacto contigo para ayudarte con el proceso.\n\n"
                            "🔹 **Botones disponibles:**\n"
                            "• **Cerrar** – Elimina el ticket.\n"
                            "• **Reclamar** – Asigna el ticket a un staff.\n"
                            "• **Bloquear/Desbloquear** – Gestiona el permiso de escritura del usuario.",
                color=Color.blue()
            )
            embed.set_footer(text=get_footer())
            view = TicketControlView(channel.id)
            await channel.send(f"{user.mention} <@&{Config.STAFF_GENERAL}>", embed=embed, view=view)
            await interaction.response.send_message(f"✅ Ticket creado: {channel.mention}", ephemeral=True)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error al crear el ticket: {e}", ephemeral=True)

class TicketSelectMenu(ui.Select):
    def __init__(self):
        self.options_data = [
            {"label": "General", "value": "general", "desc": "Cualquier consulta general", "emoji": "📩"},
            {"label": "Fundación", "value": "fundacion", "desc": "Asuntos de la fundación", "emoji": "🏛️"},
            {"label": "Facciones Legales", "value": "legales", "desc": "Ayuda con facciones legales", "emoji": "⚖️"},
            {"label": "Facciones Ilegales", "value": "ilegales", "desc": "Ayuda con facciones ilegales", "emoji": "🔫"},
            {"label": "Reportes", "value": "reportes", "desc": "Reportar a un usuario", "emoji": "📢"},
            {"label": "Empresas", "value": "empresas", "desc": "Asuntos empresariales", "emoji": "🏢"},
            {"label": "Donación", "value": "donacion", "desc": "Sobre donaciones y beneficios", "emoji": "💎"},
            {"label": "Incidencias Técnicas", "value": "incidencias", "desc": "Problemas técnicos", "emoji": "🔧"},
        ]
        options = [
            SelectOption(
                label=d["label"],
                value=d["value"],
                description=d["desc"],
                emoji=d["emoji"]
            ) for d in self.options_data
        ]
        super().__init__(placeholder="Selecciona una categoría", options=options, custom_id="ticket_select")

    async def callback(self, interaction: Interaction):
        selected_value = self.values[0]
        selected_data = next((d for d in self.options_data if d["value"] == selected_value), None)
        if not selected_data:
            await interaction.response.send_message("Categoría no válida.", ephemeral=True)
            return

        category_mapping = {
            "general": Config.CAT_TICKETS_GENERAL,
            "fundacion": Config.CAT_TICKETS_FUNDACION,
            "legales": Config.CAT_TICKETS_FACCIONES_LEGALES,
            "ilegales": Config.CAT_TICKETS_FACCIONES_ILEGALES,
            "reportes": Config.CAT_TICKETS_REPORTES,
            "empresas": Config.CAT_TICKETS_EMPRESAS,
            "donacion": Config.CAT_TICKETS_DONACION,
            "incidencias": Config.CAT_TICKETS_INCIDENCIAS_TECNICAS,
        }
        category_id = category_mapping.get(selected_value)
        if not category_id:
            await interaction.response.send_message("Categoría no válida.", ephemeral=True)
            return

        guild = interaction.guild
        user = interaction.user
        try:
            cat_name = selected_data["label"].replace(" ", "-").lower()
            channel_name = f"ticket-{cat_name}-{user.name[:10]}"
            topic = f"Ticket abierto por {user.mention} a las {datetime.datetime.utcnow().strftime('%H:%M UTC')} en la categoría {selected_data['label']}"

            channel = await create_ticket_channel(guild, category_id, user, channel_name, topic)
            embed = Embed(
                title=f"🎫 Ticket {selected_data['label']}",
                description=f"**Usuario:** {user.mention}\n"
                            f"**Tipo:** {selected_data['label']}\n\n"
                            "📌 **Instrucciones:**\n"
                            "• Explica tu problema o consulta detalladamente.\n"
                            "• Un staff te atenderá en breve.\n\n"
                            "🔹 **Botones disponibles:**\n"
                            "• **Cerrar** – Elimina el ticket.\n"
                            "• **Reclamar** – Asigna el ticket a un staff.\n"
                            "• **Bloquear/Desbloquear** – Gestiona el permiso de escritura del usuario.",
                color=Color.blue()
            )
            embed.set_footer(text=get_footer())
            view = TicketControlView(channel.id)
            await channel.send(f"{user.mention} <@&{Config.STAFF_GENERAL}>", embed=embed, view=view)
            await interaction.response.send_message(f"✅ Ticket creado: {channel.mention}", ephemeral=True)
        except ValueError as e:
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error al crear el ticket: {e}", ephemeral=True)

class TicketPanelView(ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(TicketSelectMenu())

class TicketControlView(ui.View):
    def __init__(self, channel_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id

    @ui.button(label="Cerrar", style=ButtonStyle.danger, custom_id="ticket_close")
    async def close_ticket(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para cerrar este ticket.", ephemeral=True)
            return
        channel = interaction.channel
        if channel.id != self.channel_id:
            await interaction.response.send_message("Este botón no pertenece a este canal.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return

        data = ticket_data.get(self.channel_id, {})
        owner_id = data.get("owner_id")
        claimed_by = data.get("claimed_by")

        data["closed"] = True
        ticket_data[self.channel_id] = data

        await Logger.archive_ticket(channel, interaction.user)

        if owner_id:
            owner = interaction.guild.get_member(owner_id)
            if owner:
                staff_id = claimed_by if claimed_by else interaction.user.id
                embed_val = Embed(
                    title="📝 ¡Gracias por usar nuestro sistema de tickets!",
                    description="Nos gustaría conocer tu opinión sobre la atención recibida.\n"
                                "Por favor, pulsa el botón para valorar al staff que te atendió.\n"
                                "Tienes **5 minutos** para hacerlo.",
                    color=Color.blue()
                )
                embed_val.set_footer(text=get_footer())
                view = ValorarView(owner, staff_id, self.channel_id)
                await send_dm(owner, embed=embed_val, view=view)

        await interaction.response.send_message("Cerrando ticket...")
        await channel.delete()
        ticket_data.pop(self.channel_id, None)

    @ui.button(label="Reclamar", style=ButtonStyle.primary, custom_id="ticket_claim")
    async def claim_ticket(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para reclamar este ticket.", ephemeral=True)
            return
        channel = interaction.channel
        if channel.id != self.channel_id:
            await interaction.response.send_message("Este botón no pertenece a este canal.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(self.channel_id)
        if not data:
            ticket_data[self.channel_id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[self.channel_id]
        if data["claimed_by"]:
            await interaction.response.send_message(f"Este ticket ya ha sido reclamado por <@{data['claimed_by']}>.", ephemeral=True)
            return
        data["claimed_by"] = interaction.user.id
        embed = Embed(
            title="📌 Ticket reclamado",
            description=f"**{interaction.user.mention}** ha reclamado este ticket y se hará cargo del caso.\n"
                        "Puedes esperar una respuesta en breve.",
            color=Color.green()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Has reclamado este ticket.", ephemeral=True)

    @ui.button(label="Bloquear", style=ButtonStyle.secondary, custom_id="ticket_lock")
    async def lock_ticket(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para bloquear este ticket.", ephemeral=True)
            return
        channel = interaction.channel
        if channel.id != self.channel_id:
            await interaction.response.send_message("Este botón no pertenece a este canal.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(self.channel_id)
        if not data:
            ticket_data[self.channel_id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[self.channel_id]
        if data["locked"]:
            await interaction.response.send_message("El ticket ya está bloqueado.", ephemeral=True)
            return
        data["locked"] = True
        guild = interaction.guild
        overwrites = channel.overwrites
        for target, overwrite in overwrites.items():
            if isinstance(target, discord.Role) and target == guild.default_role:
                overwrite.send_messages = False
            elif isinstance(target, discord.Member) and target != interaction.user and not is_staff(target):
                overwrite.send_messages = False
        await channel.edit(overwrites=overwrites)
        embed = Embed(
            title="🔒 Ticket bloqueado",
            description=f"El ticket ha sido bloqueado por **{interaction.user.mention}**.\n"
                        "En adelante, solo el personal del staff podrá escribir en este canal.\n"
                        "Usa el botón **Desbloquear** para restaurar los permisos.",
            color=Color.orange()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Ticket bloqueado.", ephemeral=True)

    @ui.button(label="Desbloquear", style=ButtonStyle.success, custom_id="ticket_unlock")
    async def unlock_ticket(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para desbloquear este ticket.", ephemeral=True)
            return
        channel = interaction.channel
        if channel.id != self.channel_id:
            await interaction.response.send_message("Este botón no pertenece a este canal.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(self.channel_id)
        if not data:
            ticket_data[self.channel_id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[self.channel_id]
        if not data["locked"]:
            await interaction.response.send_message("El ticket no está bloqueado.", ephemeral=True)
            return
        data["locked"] = False
        guild = interaction.guild
        overwrites = channel.overwrites
        for target, overwrite in overwrites.items():
            if isinstance(target, discord.Role) and target == guild.default_role:
                overwrite.send_messages = True
            elif isinstance(target, discord.Member) and not is_staff(target):
                overwrite.send_messages = True
        await channel.edit(overwrites=overwrites)
        embed = Embed(
            title="🔓 Ticket desbloqueado",
            description=f"El ticket ha sido desbloqueado por **{interaction.user.mention}**.\n"
                        "El usuario ya puede volver a escribir en el canal.",
            color=Color.green()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Ticket desbloqueado.", ephemeral=True)

class VerificationReviewView(ui.View):
    def __init__(self, user_id: int, roblox_name: str, roblox_id: int, answers: dict, analysis: dict):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.roblox_name = roblox_name
        self.roblox_id = roblox_id
        self.answers = answers
        self.analysis = analysis

    @ui.button(label="Aceptar", style=ButtonStyle.success, custom_id="verify_accept")
    async def accept_verification(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para realizar esta acción.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
            return
        member = guild.get_member(self.user_id)
        if not member:
            await interaction.response.send_message("El usuario ya no está en el servidor.", ephemeral=True)
            return

        verificado = guild.get_role(Config.VERIFICADO)
        no_verificado = guild.get_role(Config.NO_VERIFICADO)
        if verificado:
            await member.add_roles(verificado)
        if no_verificado and no_verificado in member.roles:
            await member.remove_roles(no_verificado)
        try:
            await member.edit(nick=f"Civil | {self.roblox_name}")
        except:
            pass

        avatar_url = await get_roblox_avatar(self.roblox_id)
        if not avatar_url:
            avatar_url = "https://www.roblox.com/asset-thumbnail/image?assetId=0&width=420&height=420&format=png"

        embed_dm = Embed(
            title="✅ ¡Verificación aprobada!",
            description=f"**Bienvenido/a a Cádiz RP, {member.mention}.**\n\n"
                        "Ya tienes acceso al servidor. Recuerda leer las normas y disfrutar del roleo.\n"
                        "Si necesitas ayuda, abre un ticket.",
            color=Color.green()
        )
        embed_dm.add_field(name="👤 Usuario de Roblox", value=f"{self.roblox_name} (ID: {self.roblox_id})", inline=False)
        embed_dm.set_thumbnail(url=avatar_url)
        embed_dm.set_footer(text=get_footer())
        await send_dm(member, "", embed=embed_dm)

        embed_original = interaction.message.embeds[0] if interaction.message.embeds else Embed(title="Revisión de verificación")
        embed_original.description = f"**Estado:** ✅ Aceptado por {interaction.user.mention}\n**Usuario:** {member.mention}\n**Roblox:** {self.roblox_name} (ID: {self.roblox_id})"
        embed_original.set_thumbnail(url=avatar_url)
        embed_original.color = Color.green()
        embed_original.set_footer(text=get_footer())
        await interaction.message.edit(embed=embed_original, view=None)
        await interaction.response.send_message("Usuario verificado correctamente.", ephemeral=True)

    @ui.button(label="Denegar", style=ButtonStyle.danger, custom_id="verify_deny")
    async def deny_verification(self, interaction: Interaction, button: ui.Button):
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos para realizar esta acción.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
            return
        member = guild.get_member(self.user_id)
        if member:
            embed_dm = Embed(
                title="❌ Verificación denegada",
                description="Lamentamos informarte que tu solicitud de verificación ha sido **rechazada**.\n"
                            "Si crees que es un error, abre un ticket para solicitar una revisión.",
                color=Color.red()
            )
            embed_dm.set_footer(text=get_footer())
            await send_dm(member, "", embed=embed_dm)
        embed_original = interaction.message.embeds[0] if interaction.message.embeds else Embed(title="Revisión de verificación")
        embed_original.description = f"**Estado:** ❌ Denegado por {interaction.user.mention}\n**Usuario:** {member.mention if member else 'Usuario no encontrado'}\n**Roblox:** {self.roblox_name}"
        embed_original.color = Color.red()
        embed_original.set_footer(text=get_footer())
        await interaction.message.edit(embed=embed_original, view=None)
        await interaction.response.send_message("Verificación denegada.", ephemeral=True)

# ===================== EVENTOS DE LOGS =====================
@bot.event
async def on_ready():
    await db.init()
    print(f"Bot conectado como {bot.user}")
    print(f"Intents activos: reactions={intents.reactions}, members={intents.members}, message_content={intents.message_content}")
    try:
        synced = await bot.tree.sync()
        print(f"Comandos sincronizados: {len(synced)}")
    except Exception as e:
        print(f"Error al sincronizar comandos: {e}")

@bot.event
async def on_member_join(member):
    embed = Logger.create_base_embed(
        title="👤 Miembro unido",
        description=f"**Miembro:** {member.mention} ({member.id})\n"
                    f"**Cuenta creada:** {member.created_at.strftime('%d/%m/%Y %H:%M:%S')}",
        color=Color.green(),
        target=f"{member.mention} ({member.id})"
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    await Logger.send_log(member.guild, Config.LOG_MIEMBROS, embed)

    guild = member.guild
    channel = guild.get_channel(Config.CH_BIENVENIDAS)
    if channel:
        human_members = sum(1 for m in guild.members if not m.bot)
        embed_bienvenida = Embed(
            title="👋 ¡Bienvenido/a a Cádiz RP!",
            description=f"**{member.mention}**, te damos la bienvenida a **Cádiz RP**.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "📌 **Pasos principales:**\n\n"
                        "1️⃣ **Verifícate** en el canal <#1505926635239768074>.\n"
                        "2️⃣ **Crea tu DNI** en <#1452608368069054480>.\n"
                        "3️⃣ **Lee las normas** en <#1452608366860959830>.\n"
                        "4️⃣ **¡Empieza a rolear!**\n"
                        "Tenemos trabajos: Policía, Emergencias, Conservación, Criminal y más.\n\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🎟️ **¿Problemas?** Abre un ticket en <#1452632044470538443>.\n\n"
                        f"📊 **Miembros humanos:** {human_members}\n"
                        "¡Disfruta del servidor!",
            color=Color.gold()
        )
        embed_bienvenida.set_thumbnail(url=member.display_avatar.url)
        embed_bienvenida.set_footer(text=get_footer())
        await channel.send(embed=embed_bienvenida)

@bot.event
async def on_member_remove(member):
    embed = Logger.create_base_embed(
        title="👤 Miembro salido",
        description=f"**Miembro:** {member.mention} ({member.id})\n"
                    f"**Rol más alto:** {member.top_role.mention if member.top_role else 'Ninguno'}",
        color=Color.red(),
        target=f"{member.mention} ({member.id})"
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    await Logger.send_log(member.guild, Config.LOG_MIEMBROS, embed)

@bot.event
async def on_member_update(before, after):
    if before.guild is None:
        return
    if before.nick != after.nick:
        embed = Logger.create_base_embed(
            title="✏️ Apodo cambiado",
            description=f"**Miembro:** {after.mention} ({after.id})\n"
                        f"**Antes:** {before.nick if before.nick else 'Ninguno'}\n"
                        f"**Después:** {after.nick if after.nick else 'Ninguno'}",
            color=Color.blue(),
            target=f"{after.mention} ({after.id})"
        )
        await Logger.send_log(after.guild, Config.LOG_MIEMBROS, embed)

    if before.roles != after.roles:
        added = [r for r in after.roles if r not in before.roles]
        removed = [r for r in before.roles if r not in after.roles]
        desc = f"**Miembro:** {after.mention} ({after.id})\n"
        if added:
            desc += f"**Roles añadidos:** {', '.join([r.mention for r in added])}\n"
        if removed:
            desc += f"**Roles eliminados:** {', '.join([r.mention for r in removed])}"
        embed = Logger.create_base_embed(
            title="🔄 Roles actualizados",
            description=desc,
            color=Color.gold(),
            target=f"{after.mention} ({after.id})"
        )
        await Logger.send_log(after.guild, Config.LOG_MIEMBROS, embed)

@bot.event
async def on_guild_role_create(role):
    embed = Logger.create_base_embed(
        title="➕ Rol creado",
        description=f"**Rol:** {role.mention} ({role.id})\n"
                    f"**Color:** {role.color}\n"
                    f"**Mencionable:** {role.mentionable}",
        color=Color.green(),
        target=f"{role.mention} ({role.id})"
    )
    await Logger.send_log(role.guild, Config.LOG_ROLES, embed)

@bot.event
async def on_guild_role_delete(role):
    embed = Logger.create_base_embed(
        title="➖ Rol eliminado",
        description=f"**Rol:** {role.name} ({role.id})\n"
                    f"**Color:** {role.color}",
        color=Color.red(),
        target=f"{role.name} ({role.id})"
    )
    await Logger.send_log(role.guild, Config.LOG_ROLES, embed)

@bot.event
async def on_guild_role_update(before, after):
    changes = []
    if before.name != after.name:
        changes.append(f"**Nombre:** {before.name} → {after.name}")
    if before.color != after.color:
        changes.append(f"**Color:** {before.color} → {after.color}")
    if before.mentionable != after.mentionable:
        changes.append(f"**Mencionable:** {before.mentionable} → {after.mentionable}")
    if changes:
        embed = Logger.create_base_embed(
            title="🔄 Rol actualizado",
            description=f"**Rol:** {after.mention} ({after.id})\n" + "\n".join(changes),
            color=Color.blue(),
            target=f"{after.mention} ({after.id})"
        )
        await Logger.send_log(after.guild, Config.LOG_ROLES, embed)

@bot.event
async def on_guild_emojis_update(guild, before, after):
    added = [e for e in after if e not in before]
    removed = [e for e in before if e not in after]
    for emoji in added:
        embed = Logger.create_base_embed(
            title="➕ Emoji creado",
            description=f"**Nombre:** {emoji.name} ({emoji.id})\n"
                        f"**Animado:** {emoji.animated}",
            color=Color.green(),
            target=f"{emoji.name} ({emoji.id})"
        )
        embed.set_thumbnail(url=str(emoji.url))
        await Logger.send_log(guild, Config.LOG_EMOJIS, embed)
    for emoji in removed:
        embed = Logger.create_base_embed(
            title="➖ Emoji eliminado",
            description=f"**Nombre:** {emoji.name} ({emoji.id})",
            color=Color.red(),
            target=f"{emoji.name} ({emoji.id})"
        )
        await Logger.send_log(guild, Config.LOG_EMOJIS, embed)

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot or reaction.message.guild is None:
        return
    embed = Logger.create_base_embed(
        title="➕ Reacción añadida",
        description=f"**Usuario:** {user.mention} ({user.id})\n"
                    f"**Mensaje:** [Ir al mensaje]({reaction.message.jump_url})\n"
                    f"**Reacción:** {reaction.emoji}",
        color=Color.green(),
        author=user,
        target=f"{user.mention} ({user.id})"
    )
    await Logger.send_log(reaction.message.guild, Config.LOG_EMOJIS, embed)

@bot.event
async def on_reaction_remove(reaction, user):
    if user.bot or reaction.message.guild is None:
        return
    embed = Logger.create_base_embed(
        title="➖ Reacción eliminada",
        description=f"**Usuario:** {user.mention} ({user.id})\n"
                    f"**Mensaje:** [Ir al mensaje]({reaction.message.jump_url})\n"
                    f"**Reacción:** {reaction.emoji}",
        color=Color.red(),
        author=user,
        target=f"{user.mention} ({user.id})"
    )
    await Logger.send_log(reaction.message.guild, Config.LOG_EMOJIS, embed)

@bot.event
async def on_guild_channel_create(channel):
    embed = Logger.create_base_embed(
        title="➕ Canal creado",
        description=f"**Canal:** {channel.mention} ({channel.id})\n"
                    f"**Tipo:** {channel.type}\n"
                    f"**Categoría:** {channel.category.name if channel.category else 'Ninguna'}",
        color=Color.green(),
        target=f"{channel.mention} ({channel.id})"
    )
    await Logger.send_log(channel.guild, Config.LOG_CANALES, embed)

@bot.event
async def on_guild_channel_delete(channel):
    embed = Logger.create_base_embed(
        title="➖ Canal eliminado",
        description=f"**Canal:** {channel.name} ({channel.id})\n"
                    f"**Tipo:** {channel.type}\n"
                    f"**Categoría:** {channel.category.name if channel.category else 'Ninguna'}",
        color=Color.red(),
        target=f"{channel.name} ({channel.id})"
    )
    await Logger.send_log(channel.guild, Config.LOG_CANALES, embed)

@bot.event
async def on_guild_channel_update(before, after):
    changes = []
    if before.name != after.name:
        changes.append(f"**Nombre:** {before.name} → {after.name}")
    if before.category != after.category:
        changes.append(f"**Categoría:** {before.category.name if before.category else 'Ninguna'} → {after.category.name if after.category else 'Ninguna'}")
    if changes:
        embed = Logger.create_base_embed(
            title="🔄 Canal actualizado",
            description=f"**Canal:** {after.mention} ({after.id})\n" + "\n".join(changes),
            color=Color.blue(),
            target=f"{after.mention} ({after.id})"
        )
        await Logger.send_log(after.guild, Config.LOG_CANALES, embed)

@bot.event
async def on_invite_create(invite):
    embed = Logger.create_base_embed(
        title="➕ Invitación creada",
        description=f"**Creador:** {invite.inviter.mention if invite.inviter else 'Desconocido'}\n"
                    f"**Código:** {invite.code}\n"
                    f"**Canal:** {invite.channel.mention}\n"
                    f"**Usos máximos:** {invite.max_uses if invite.max_uses else 'Ilimitado'}\n"
                    f"**Expira:** {invite.expires_at.strftime('%d/%m/%Y %H:%M') if invite.expires_at else 'Nunca'}",
        color=Color.green(),
        author=invite.inviter,
        target=f"{invite.code}"
    )
    await Logger.send_log(invite.guild, Config.LOG_INVITACIONES, embed)

@bot.event
async def on_invite_delete(invite):
    embed = Logger.create_base_embed(
        title="➖ Invitación eliminada",
        description=f"**Código:** {invite.code}\n"
                    f"**Canal:** {invite.channel.mention if invite.channel else 'Desconocido'}\n"
                    f"**Usos:** {invite.uses}",
        color=Color.red(),
        target=f"{invite.code}"
    )
    await Logger.send_log(invite.guild, Config.LOG_INVITACIONES, embed)

@bot.event
async def on_member_ban(guild: discord.Guild, user: discord.User):
    moderator = await get_audit_log_moderator(guild, discord.AuditLogAction.ban, user.id)
    desc = f"**Usuario baneado:** {user.mention} ({user.id})"
    if moderator:
        desc += f"\n**Moderador:** {moderator.mention} ({moderator.id})"
    embed = Logger.create_base_embed(
        title="🔨 Usuario baneado",
        description=desc,
        color=Color.dark_red(),
        author=moderator,
        target=f"{user.mention} ({user.id})"
    )
    await Logger.send_log(guild, Config.LOG_MODERACION, embed)

@bot.event
async def on_member_unban(guild: discord.Guild, user: discord.User):
    moderator = await get_audit_log_moderator(guild, discord.AuditLogAction.unban, user.id)
    desc = f"**Usuario desbaneado:** {user.mention} ({user.id})"
    if moderator:
        desc += f"\n**Moderador:** {moderator.mention} ({moderator.id})"
    embed = Logger.create_base_embed(
        title="🔓 Usuario desbaneado",
        description=desc,
        color=Color.green(),
        author=moderator,
        target=f"{user.mention} ({user.id})"
    )
    await Logger.send_log(guild, Config.LOG_MODERACION, embed)

@bot.event
async def on_message_delete(message):
    if message.guild is None or message.author.bot:
        return
    embed = Logger.create_base_embed(
        title="🗑️ Mensaje eliminado",
        description=f"**Autor del mensaje:** {message.author.mention} ({message.author.id})\n"
                    f"**Canal:** {message.channel.mention}\n"
                    f"**Contenido:** {message.content if message.content else '(Sin contenido)'}",
        color=Color.red(),
        author=message.author,
        target=f"{message.author.mention} ({message.author.id})"
    )
    if message.attachments:
        embed.add_field(name="Archivos adjuntos", value="\n".join([a.url for a in message.attachments]), inline=False)
    await Logger.send_log(message.guild, Config.LOG_MENSAJES, embed)

@bot.event
async def on_message_edit(before, after):
    if before.guild is None or before.author.bot or before.content == after.content:
        return
    embed = Logger.create_base_embed(
        title="✏️ Mensaje editado",
        description=f"**Autor:** {before.author.mention} ({before.author.id})\n"
                    f"**Canal:** {before.channel.mention}\n"
                    f"**Antes:** {before.content if before.content else '(Vacío)'}\n"
                    f"**Después:** {after.content if after.content else '(Vacío)'}",
        color=Color.orange(),
        author=before.author,
        target=f"{before.author.mention} ({before.author.id})"
    )
    await Logger.send_log(before.guild, Config.LOG_MENSAJES, embed)

@bot.event
async def on_bulk_message_delete(messages):
    if not messages:
        return
    guild = messages[0].guild
    if guild is None:
        return
    channel = messages[0].channel
    embed = Logger.create_base_embed(
        title="📦 Mensajes eliminados en masa",
        description=f"**Canal:** {channel.mention}\n"
                    f"**Cantidad:** {len(messages)} mensajes",
        color=Color.dark_red(),
        target=f"Canal {channel.mention}"
    )
    await Logger.send_log(guild, Config.LOG_MENSAJES, embed)

# ===================== PROCESO DE VERIFICACIÓN POR DM (MEJORADO) =====================
@bot.event
async def on_message(message):
    try:
        # Solo procesar mensajes DM de usuarios en sesión de verificación
        if isinstance(message.channel, discord.DMChannel) and message.author.id in verification_sessions:
            user_id = message.author.id
            session = verification_sessions[user_id]
            step = session.get("step")
            answers = session.get("answers", {})

            print(f"[Verificación] Paso {step} - Usuario {message.author.name} respondió: {message.content[:50]}...")

            if step == 1:
                username = message.content.strip()
                try:
                    roblox_id = await check_roblox_user(username)
                except Exception as e:
                    await message.channel.send("❌ Error al verificar usuario de Roblox. Intenta de nuevo más tarde.")
                    print(f"[Error] API Roblox falló: {e}")
                    return

                if not roblox_id:
                    await message.channel.send("❌ El usuario de Roblox no existe. Por favor, vuelve a iniciar el proceso con el botón COMENZAR.")
                    verification_sessions.pop(user_id, None)
                    return

                answers["roblox_user"] = username
                answers["roblox_id"] = roblox_id
                session["step"] = 2

                avatar_url = await get_roblox_avatar(roblox_id)
                if avatar_url:
                    embed_confirm = Embed(
                        title="✅ Usuario de Roblox verificado",
                        description=f"**Nombre:** {username}\n**ID:** {roblox_id}",
                        color=Color.green()
                    )
                    embed_confirm.set_thumbnail(url=avatar_url)
                    embed_confirm.set_footer(text=get_footer())
                    await message.channel.send(embed=embed_confirm)
                else:
                    await message.channel.send(f"✅ Usuario de Roblox verificado (ID: {roblox_id}).")

                await message.channel.send("**Pregunta 2:** ¿Cómo te has metido al servidor?")

            elif step == 2:
                answers["como_metiste"] = message.content.strip()
                session["step"] = 3
                await message.channel.send("**Pregunta 3:** Del 1 al 10, ¿cuánto sabes rolear?")

            elif step == 3:
                answers["nivel_roleo"] = message.content.strip()
                session["step"] = 4
                await message.channel.send("**Pregunta 4:** ¿Qué significa MG? Pon un ejemplo.")

            elif step == 4:
                answers["mg"] = message.content.strip()
                session["step"] = 5
                await message.channel.send("**Pregunta 5:** ¿Qué significa PG? Pon un ejemplo.")

            elif step == 5:
                answers["pg"] = message.content.strip()
                session["step"] = 6
                await message.channel.send("**Pregunta 6:** ¿Qué harías si ves a alguien haciendo antirol?")

            elif step == 6:
                answers["antirol"] = message.content.strip()
                session["step"] = 7
                await message.channel.send("**Pregunta 7:** ¿Qué te gustaría ser dentro del servidor?")

            elif step == 7:
                answers["aspiracion"] = message.content.strip()
                session["step"] = 8
                await message.channel.send("**Pregunta 8:** ¿Una vez verificado aceptas que no podrás realizar ningún antirol? (responde sí o no)")

            elif step == 8:
                respuesta = message.content.strip().lower()
                if respuesta not in ["sí", "si", "no"]:
                    await message.channel.send("Por favor, responde con 'sí' o 'no'.")
                    return

                answers["acepta_antirol"] = respuesta in ["sí", "si"]
                if session.get("timeout_task"):
                    session["timeout_task"].cancel()

                analysis = VerificationEvaluator.evaluate_all(answers)

                guild = bot.get_guild(1452608365812514999)  # CAMBIA POR EL ID DE TU SERVIDOR
                if not guild:
                    guild = bot.guilds[0]
                channel_revision = guild.get_channel(Config.CH_REVISIONES_VERIFICACION)
                if channel_revision:
                    preguntas_respuestas = (
                        f"**P1: Usuario de Roblox**\n{answers['roblox_user']}\n\n"
                        f"**P2: ¿Cómo te has metido al servidor?**\n{answers['como_metiste']}\n\n"
                        f"**P3: Del 1 al 10, ¿cuánto sabes rolear?**\n{answers['nivel_roleo']}\n\n"
                        f"**P4: ¿Qué significa MG? Pon un ejemplo.**\n{answers['mg']}\n\n"
                        f"**P5: ¿Qué significa PG? Pon un ejemplo.**\n{answers['pg']}\n\n"
                        f"**P6: ¿Qué harías si ves a alguien haciendo antirol?**\n{answers['antirol']}\n\n"
                        f"**P7: ¿Qué te gustaría ser dentro del servidor?**\n{answers['aspiracion']}\n\n"
                        f"**P8: ¿Una vez verificado aceptas que no podrás realizar ningún antirol?**\n{'Sí' if answers['acepta_antirol'] else 'No'}"
                    )
                    embed = Embed(
                        title="📋 Revisión de Verificación",
                        description=f"**Usuario:** {message.author.mention}\n**Roblox:** {answers['roblox_user']}",
                        color=Color.blue()
                    )
                    embed.add_field(name="📋 Respuestas del formulario:", value=preguntas_respuestas, inline=False)
                    embed.add_field(
                        name="📊 Análisis automático — REVISAR",
                        value=f"**Puntuación total:** {analysis['total']}/95\n"
                              f"**Metagaming:** {analysis['mg']}/20 pts\n"
                              f"**Powergaming:** {analysis['pg']}/20 pts\n"
                              f"**Fail RP:** {analysis['failrp']}/15 pts\n"
                              f"**Situación práctica:** {analysis['situation']}/15 pts\n"
                              f"**Compromiso:** {analysis['compromiso']}/10 pts\n"
                              f"**Bonus calidad:** {analysis['bonus']}/15 pts\n"
                              f"**Decisión sugerida:** {analysis['decision']}",
                        inline=False
                    )
                    if analysis['alerts']:
                        embed.add_field(
                            name="⚠️ Alertas",
                            value="\n".join(f"• {alert}" for alert in analysis['alerts']),
                            inline=False
                        )
                    embed.add_field(name="📌 Motivo sugerido", value=analysis['motivo'], inline=False)
                    embed.set_footer(text=get_footer())
                    view = VerificationReviewView(
                        user_id=message.author.id,
                        roblox_name=answers['roblox_user'],
                        roblox_id=answers['roblox_id'],
                        answers=answers,
                        analysis=analysis
                    )
                    await channel_revision.send(f"<@&{Config.STAFF_GENERAL}>", embed=embed, view=view)
                    await message.channel.send("✅ Tu verificación ha sido enviada a revisión. Espera la respuesta del staff.")
                else:
                    await message.channel.send("❌ No se encontró el canal de revisiones. Contacta con un administrador.")

                verification_sessions.pop(user_id, None)

            # Guardar cambios
            verification_sessions[user_id] = session
            return  # Importante: no procesar como comando

    except Exception as e:
        print(f"[ERROR en verificación] {e}")
        traceback.print_exc()
        try:
            await message.channel.send("❌ Ocurrió un error al procesar tu respuesta. Por favor, inicia de nuevo el proceso.")
        except:
            pass
        if message.author.id in verification_sessions:
            verification_sessions.pop(message.author.id, None)

    # Procesar comandos (solo para mensajes de servidor, no DM)
    await bot.process_commands(message)

# ===================== COMANDOS =====================
@bot.tree.command(name="enviar-panel-verificacion", description="Envía el panel de verificación con botones")
@app_commands.default_permissions(administrator=True)
async def enviar_panel_verificacion(interaction: Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
        return
    channel = guild.get_channel(Config.CH_VERIFICACION_PANEL)
    if not channel:
        await interaction.response.send_message("No se encontró el canal de verificación.", ephemeral=True)
        return
    embed = Embed(
        title="🔐 Panel de Verificación",
        description="**Bienvenido al sistema de verificación de Cádiz RP.**\n\n"
                    "Para poder acceder al servidor y comenzar a rolear, debes completar el proceso de verificación.\n\n"
                    "📌 **¿Cómo funciona?**\n"
                    "• Presiona **COMENZAR** para iniciar el cuestionario.\n"
                    "• Recibirás las preguntas por mensaje privado.\n"
                    "• Responde con honestidad.\n"
                    "• Un staff revisará tus respuestas y te aprobará o denegará.\n\n"
                    "⚠️ **Si tienes problemas**, usa el botón **TICKET** para abrir un ticket de soporte.\n\n"
                    "¡Mucha suerte!",
        color=Color.gold()
    )
    embed.set_thumbnail(url=Config.SERVER_LOGO)
    embed.set_footer(text=get_footer())
    view = VerificationPanelView()
    await channel.send(embed=embed, view=view)
    await interaction.response.send_message("Panel de verificación enviado.", ephemeral=True)

@bot.tree.command(name="enviar-panel-tickets", description="Envía el panel de tickets con selector de categorías")
@app_commands.default_permissions(administrator=True)
async def enviar_panel_tickets(interaction: Interaction):
    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
        return
    embed = Embed(
        title="🎫 Sistema de Tickets",
        description="**Selecciona la categoría de tu solicitud** en el menú desplegable.\n\n"
                    "📌 **Instrucciones:**\n"
                    "• Elige la opción que mejor se ajuste a tu consulta.\n"
                    "• Se creará un canal privado para ti y el staff.\n"
                    "• Explica detalladamente tu problema o petición.\n"
                    "• Un miembro del staff te atenderá lo antes posible.\n\n"
                    "⚠️ **Importante:** No abuses del sistema. Los tickets son para asuntos importantes.",
        color=Color.blue()
    )
    embed.set_thumbnail(url=Config.SERVER_LOGO)
    embed.set_footer(text=get_footer())
    view = TicketPanelView()
    await interaction.channel.send(embed=embed, view=view)
    await interaction.response.send_message("Panel de tickets enviado.", ephemeral=True)

@bot.tree.command(name="cerrar-ticket", description="Cierra el ticket actual (solo staff)")
async def cerrar_ticket(interaction: Interaction):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Este comando solo funciona en un canal de texto.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return

        data = ticket_data.get(channel.id, {})
        owner_id = data.get("owner_id")
        claimed_by = data.get("claimed_by")
        data["closed"] = True
        ticket_data[channel.id] = data

        await Logger.archive_ticket(channel, interaction.user)

        if owner_id:
            owner = interaction.guild.get_member(owner_id)
            if owner:
                staff_id = claimed_by if claimed_by else interaction.user.id
                embed_val = Embed(
                    title="📝 ¡Gracias por usar nuestro sistema de tickets!",
                    description="Nos gustaría conocer tu opinión sobre la atención recibida.\n"
                                "Por favor, pulsa el botón para valorar al staff que te atendió.\n"
                                "Tienes **5 minutos** para hacerlo.",
                    color=Color.blue()
                )
                embed_val.set_footer(text=get_footer())
                view = ValorarView(owner, staff_id, channel.id)
                await send_dm(owner, embed=embed_val, view=view)

        await interaction.response.send_message("Cerrando ticket...")
        await channel.delete()
        ticket_data.pop(channel.id, None)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="reclamar-ticket", description="Reclama el ticket actual (solo staff)")
async def reclamar_ticket(interaction: Interaction):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Este comando solo funciona en un canal de texto.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(channel.id)
        if not data:
            ticket_data[channel.id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[channel.id]
        if data["claimed_by"]:
            await interaction.response.send_message(f"Este ticket ya ha sido reclamado por <@{data['claimed_by']}>.", ephemeral=True)
            return
        data["claimed_by"] = interaction.user.id
        embed = Embed(
            title="📌 Ticket reclamado",
            description=f"**{interaction.user.mention}** ha reclamado este ticket y se hará cargo.\n"
                        "Puedes esperar una respuesta en breve.",
            color=Color.green()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Has reclamado este ticket.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="bloquear-ticket", description="Bloquea el ticket actual (solo staff)")
async def bloquear_ticket(interaction: Interaction):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Este comando solo funciona en un canal de texto.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(channel.id)
        if not data:
            ticket_data[channel.id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[channel.id]
        if data["locked"]:
            await interaction.response.send_message("El ticket ya está bloqueado.", ephemeral=True)
            return
        data["locked"] = True
        guild = interaction.guild
        overwrites = channel.overwrites
        for target, overwrite in overwrites.items():
            if isinstance(target, discord.Role) and target == guild.default_role:
                overwrite.send_messages = False
            elif isinstance(target, discord.Member) and target != interaction.user and not is_staff(target):
                overwrite.send_messages = False
        await channel.edit(overwrites=overwrites)
        embed = Embed(
            title="🔒 Ticket bloqueado",
            description=f"El ticket ha sido bloqueado por **{interaction.user.mention}**.\n"
                        "Solo el staff puede escribir ahora.",
            color=Color.orange()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Ticket bloqueado.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="desbloquear-ticket", description="Desbloquea el ticket actual (solo staff)")
async def desbloquear_ticket(interaction: Interaction):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("Este comando solo funciona en un canal de texto.", ephemeral=True)
            return
        if not is_ticket_channel(channel):
            await interaction.response.send_message("Este canal no es un ticket válido.", ephemeral=True)
            return
        data = ticket_data.get(channel.id)
        if not data:
            ticket_data[channel.id] = {"owner_id": None, "claimed_by": None, "locked": False, "closed": False}
            data = ticket_data[channel.id]
        if not data["locked"]:
            await interaction.response.send_message("El ticket no está bloqueado.", ephemeral=True)
            return
        data["locked"] = False
        guild = interaction.guild
        overwrites = channel.overwrites
        for target, overwrite in overwrites.items():
            if isinstance(target, discord.Role) and target == guild.default_role:
                overwrite.send_messages = True
            elif isinstance(target, discord.Member) and not is_staff(target):
                overwrite.send_messages = True
        await channel.edit(overwrites=overwrites)
        embed = Embed(
            title="🔓 Ticket desbloqueado",
            description=f"El ticket ha sido desbloqueado por **{interaction.user.mention}**.\n"
                        "El usuario ya puede escribir nuevamente.",
            color=Color.green()
        )
        embed.set_footer(text=get_footer())
        await channel.send(embed=embed)
        await interaction.response.send_message("Ticket desbloqueado.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="abrir-servidor", description="Abre el servidor (solo staff)")
async def abrir_servidor(interaction: Interaction):
    if not is_staff(interaction.user):
        await interaction.response.send_message("No tienes permisos.", ephemeral=True)
        return
    await db.set_server_status("abierto")
    embed = Embed(
        title="🟢 SERVIDOR ABIERTO",
        description=f"**El servidor ha sido abierto por {interaction.user.mention}.**\n\n"
                    f"📅 **Fecha/Hora (UTC):** {get_utc_timestamp()}\n\n"
                    "🔹 Los miembros pueden acceder con normalidad.\n\n"
                    "🔢 **Codigo:** TgZHW",
        color=Color.green()
    )
    embed.set_footer(text=get_footer())
    await interaction.response.defer()
    await interaction.followup.send(
        f"<@&{Config.VERIFICADO}>",
        embed=embed,
        allowed_mentions=discord.AllowedMentions(roles=True)
    )

@bot.tree.command(name="cerrar-servidor", description="Cierra el servidor (solo staff)")
async def cerrar_servidor(interaction: Interaction):
    if not is_staff(interaction.user):
        await interaction.response.send_message("No tienes permisos.", ephemeral=True)
        return
    await db.set_server_status("cerrado")
    embed = Embed(
        title="🔴 SERVIDOR CERRADO",
        description=f"**El servidor ha sido cerrado por {interaction.user.mention}.**\n\n"
                    f"📅 **Fecha/Hora (UTC):** {get_utc_timestamp()}\n\n"
                    "🔹 El acceso está restringido temporalmente.",
        color=Color.red()
    )
    embed.set_footer(text=get_footer())
    await interaction.response.defer()
    await interaction.followup.send(
        f"<@&{Config.VERIFICADO}>",
        embed=embed,
        allowed_mentions=discord.AllowedMentions(roles=True)
    )

@bot.tree.command(name="votación-abrir", description="Inicia una votación para abrir el servidor (solo staff)")
async def votacion_abrir(interaction: Interaction):
    if not is_staff(interaction.user):
        await interaction.response.send_message("No tienes permisos.", ephemeral=True)
        return
    status = await db.get_server_status()
    if status == "abierto":
        await interaction.response.send_message("El servidor ya está abierto. No es necesario votar.", ephemeral=True)
        return

    embed = Embed(
        title="🗳️ Votación para abrir el servidor",
        description="**El equipo moderativo ha iniciado una votación para abrir el servidor.**\n\n"
                    "📌 **Reglas:**\n"
                    "• Se necesitan **5 votos** para abrir.\n"
                    "• Vota con una de las reacciones:\n"
                    "   ✅ **Si vas a rolear**\n"
                    "   ❌ **Si no vas a rolear**\n"
                    "   ⏰ **Si entras tarde**\n\n"
                    "🔹 **Participa y decide el futuro de la sesión de roleo.**",
        color=Color.purple()
    )
    embed.set_footer(text=get_footer())
    await interaction.response.defer()
    await interaction.followup.send(
        f"<@&{Config.VERIFICADO}>",
        embed=embed,
        allowed_mentions=discord.AllowedMentions(roles=True)
    )
    message = await interaction.original_response()
    await message.add_reaction("✅")
    await message.add_reaction("❌")
    await message.add_reaction("⏰")

@bot.tree.command(name="sancionar", description="Aplica una sanción a un usuario por infracción de normas (MG, PG, faltas, etc.)")
@app_commands.describe(
    usuario="Usuario a sancionar",
    motivo="Motivo de la sanción",
    tipo="Tipo de sanción",
    pruebas="Enlace a pruebas (opcional)",
    observaciones="Observaciones adicionales (opcional)"
)
@app_commands.choices(
    tipo=[
        app_commands.Choice(name="Grave", value="Grave"),
        app_commands.Choice(name="Media", value="Media"),
        app_commands.Choice(name="Leve", value="Leve"),
        app_commands.Choice(name="Verbal", value="Verbal"),
    ]
)
async def sancionar(interaction: Interaction, usuario: discord.Member, motivo: str, tipo: app_commands.Choice[str],
                    pruebas: str = "Sin pruebas", observaciones: str = "Sin observaciones"):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        sancion_id = await db.add_sancion(
            usuario_id=usuario.id,
            usuario_nombre=usuario.display_name,
            staff_id=interaction.user.id,
            staff_nombre=interaction.user.display_name,
            motivo=motivo,
            tipo=tipo.value,
            pruebas=pruebas,
            observaciones=observaciones
        )

        embed_dm = Embed(
            title="📜 Has recibido una sanción",
            description=f"**{usuario.mention}**, has sido sancionado en **Cádiz RP**.\n\n"
                        "🔹 **Detalles:**\n"
                        f"• **Motivo:** {motivo}\n"
                        f"• **Tipo:** {tipo.value}\n"
                        f"• **Pruebas:** {pruebas}\n"
                        f"• **Observaciones:** {observaciones}\n"
                        f"• **Staff:** {interaction.user.mention}\n"
                        f"• **ID de sanción:** `{sancion_id}`\n\n"
                        "Si consideras que es injusta, puedes solicitar una revisión mediante un ticket.",
            color=Color.red()
        )
        embed_dm.set_footer(text=get_footer())
        await send_dm(usuario, "", embed=embed_dm)

        guild = interaction.guild
        if guild:
            channel = guild.get_channel(Config.CH_SANCIONES_PUBLICAS)
            if channel:
                embed_public = Embed(
                    title="⚖️ Sanción aplicada",
                    description=f"**Usuario:** {usuario.mention}\n"
                                f"**Staff:** {interaction.user.mention}\n"
                                f"**Motivo:** {motivo}\n"
                                f"**Tipo:** {tipo.value}\n"
                                f"**Pruebas:** {pruebas}\n"
                                f"**Observaciones:** {observaciones}\n"
                                f"**ID de sanción:** {sancion_id}",
                    color=Color.orange()
                )
                embed_public.set_footer(text=get_footer())
                await channel.send(embed=embed_public)
        await interaction.response.send_message(f"Sanción aplicada a {usuario.display_name}. ID: {sancion_id}", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="sanciones-ver", description="Ver las sanciones que ha recibido un usuario")
@app_commands.describe(usuario="Usuario del cual ver sanciones")
async def sanciones_ver(interaction: Interaction, usuario: discord.Member):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        sanciones = await db.get_sanciones_usuario(usuario.id)
        if not sanciones:
            await interaction.response.send_message(f"{usuario.display_name} no tiene sanciones.", ephemeral=True)
            return
        embed = Embed(
            title=f"📋 Sanciones de {usuario.display_name}",
            description="Se muestran las últimas 10 sanciones.",
            color=Color.blue()
        )
        for s in sanciones:
            fecha = s["fecha"].strftime("%d/%m/%Y %H:%M")
            sancion_id = str(s['_id'])
            embed.add_field(
                name=f"ID {sancion_id} - {s['tipo']}",
                value=f"Motivo: {s['motivo']}\nStaff que sanciona: {s['staff_nombre']}\nFecha: {fecha}",
                inline=False
            )
        embed.set_footer(text=get_footer())
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="sancion-eliminar", description="Eliminar una sanción por su ID completo")
@app_commands.describe(sancion_id="ID completo de la sanción a eliminar (copia del embed)")
async def sancion_eliminar(interaction: Interaction, sancion_id: str):
    try:
        if not is_staff(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        success = await db.eliminar_sancion(sancion_id)
        if success:
            await interaction.response.send_message(f"Sanción ID `{sancion_id}` eliminada.", ephemeral=True)
        else:
            await interaction.response.send_message(f"No se encontró la sanción con ID `{sancion_id}`.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="sancionar-staff", description="Sanciona a un staff por incumplir la normativa interna (solo Fundador+)")
@app_commands.describe(
    staff_member="Staff a sancionar",
    motivo="Motivo de la sanción",
    tipo="Tipo de sanción",
    pruebas="Enlace a pruebas (opcional)",
    observaciones="Observaciones adicionales (opcional)"
)
@app_commands.choices(
    tipo=[
        app_commands.Choice(name="Grave", value="Grave"),
        app_commands.Choice(name="Media", value="Media"),
        app_commands.Choice(name="Leve", value="Leve"),
        app_commands.Choice(name="Verbal", value="Verbal"),
    ]
)
async def sancionar_staff(interaction: Interaction, staff_member: discord.Member, motivo: str, tipo: app_commands.Choice[str],
                          pruebas: str = "Sin pruebas", observaciones: str = "Sin observaciones"):
    try:
        if not is_fundador_or_higher(interaction.user):
            await interaction.response.send_message("No tienes permisos para sancionar a personal de staff.", ephemeral=True)
            return
        if not is_staff(staff_member):
            await interaction.response.send_message("El usuario no es miembro del staff.", ephemeral=True)
            return

        sancion_id = await db.add_sancion(
            usuario_id=staff_member.id,
            usuario_nombre=staff_member.display_name,
            staff_id=interaction.user.id,
            staff_nombre=interaction.user.display_name,
            motivo=f"[STAFF] {motivo}",
            tipo=tipo.value,
            pruebas=pruebas,
            observaciones=observaciones
        )

        embed_dm = Embed(
            title="📜 Sanción de staff",
            description=f"Has recibido una sanción por parte de la dirección.\n"
                        f"**Motivo:** {motivo}\n**Tipo:** {tipo.value}",
            color=Color.red()
        )
        embed_dm.set_footer(text=get_footer())
        await send_dm(staff_member, "", embed=embed_dm)

        guild = interaction.guild
        if guild:
            channel = guild.get_channel(Config.CH_SANCIONES_STAFF)
            if channel:
                embed_public = Embed(
                    title="⚖️ Sanción a staff",
                    description=f"**Staff sancionado:** {staff_member.mention}\n"
                                f"**Staff que sanciona:** {interaction.user.mention}\n"
                                f"**Motivo:** {motivo}\n"
                                f"**Tipo:** {tipo.value}\n"
                                f"**Pruebas:** {pruebas}\n"
                                f"**Observaciones:** {observaciones}\n"
                                f"**ID de sanción:** {sancion_id}",
                    color=Color.orange()
                )
                embed_public.set_footer(text=get_footer())
                await channel.send(embed=embed_public)

        await interaction.response.send_message(f"Sanción de staff aplicada a {staff_member.display_name}. ID: {sancion_id}", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="ver-sanciones-staff", description="Ver las sanciones que ha recibido un staff (solo Fundador+)")
@app_commands.describe(staff_member="Staff del cual ver sanciones recibidas")
async def ver_sanciones_staff(interaction: Interaction, staff_member: discord.Member):
    try:
        if not is_fundador_or_higher(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        if not is_staff(staff_member):
            await interaction.response.send_message("El usuario no es miembro del staff.", ephemeral=True)
            return

        sanciones = await db.get_sanciones_usuario(staff_member.id)

        if not sanciones:
            await interaction.response.send_message(f"{staff_member.display_name} no tiene sanciones de staff recibidas.", ephemeral=True)
            return

        embed = Embed(
            title=f"📋 Sanciones recibidas por {staff_member.display_name}",
            color=Color.blue()
        )
        for s in sanciones:
            fecha = s["fecha"].strftime("%d/%m/%Y %H:%M")
            sancion_id = str(s['_id'])
            embed.add_field(
                name=f"ID {sancion_id} - {s['tipo']}",
                value=f"Motivo: {s['motivo']}\nStaff que sanciona: {s['staff_nombre']}\nFecha: {fecha}",
                inline=False
            )
        embed.set_footer(text=get_footer())
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="eliminar-sancion-staff", description="Eliminar una sanción de staff por su ID completo (solo Fundador+)")
@app_commands.describe(sancion_id="ID completo de la sanción a eliminar (copia del embed)")
async def eliminar_sancion_staff(interaction: Interaction, sancion_id: str):
    try:
        if not is_fundador_or_higher(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        success = await db.eliminar_sancion(sancion_id)
        if success:
            await interaction.response.send_message(f"Sanción de staff ID `{sancion_id}` eliminada.", ephemeral=True)
        else:
            await interaction.response.send_message(f"No se encontró la sanción con ID `{sancion_id}`.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="informe-staff", description="Genera un informe de sanciones emitidas por cada staff (solo Fundador+)")
async def informe_staff(interaction: Interaction):
    try:
        if not is_fundador_or_higher(interaction.user):
            await interaction.response.send_message("No tienes permisos.", ephemeral=True)
            return
        sanciones = await db.get_all_sanciones()
        if not sanciones:
            await interaction.response.send_message("No hay sanciones registradas.", ephemeral=True)
            return
        staff_stats = {}
        for s in sanciones:
            staff_id = s["staff_id"]
            if staff_id not in staff_stats:
                staff_stats[staff_id] = {"nombre": s["staff_nombre"], "count": 0}
            staff_stats[staff_id]["count"] += 1
        embed = Embed(
            title="📊 Informe de sanciones por staff",
            description="Cantidad total de sanciones emitidas por cada miembro del staff.",
            color=Color.gold()
        )
        for staff_id, data in staff_stats.items():
            member = interaction.guild.get_member(staff_id)
            nombre = member.display_name if member else data["nombre"]
            embed.add_field(name=nombre, value=f"{data['count']} sanciones", inline=False)
        embed.set_footer(text=get_footer())
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"Error: {e}", ephemeral=True)

@bot.tree.command(name="citar", description="Cita a un usuario a soporte")
@app_commands.describe(
    usuario="Usuario a citar",
    motivo="Motivo de la citación",
    intento="Número de intento (1-3)"
)
async def citar(interaction: Interaction, usuario: discord.Member, motivo: str, intento: app_commands.Range[int, 1, 3]):
    if not is_staff(interaction.user):
        await interaction.response.send_message("No tienes permisos.", ephemeral=True)
        return

    embed_dm = Embed(
        title="📞 Has sido citado a soporte",
        description="Has sido citado por el Staff de **Cádiz RP**.\n\n"
                    "Por favor, únete al canal de voz **📞 Esperando Soporte** para ser atendido.\n\n"
                    "🔹 **Motivo:**\n"
                    f"{motivo}\n\n"
                    f"✅ **Citado por:** {interaction.user.mention}",
        color=Color.blue()
    )
    embed_dm.set_footer(text=get_footer())
    await send_dm(usuario, "", embed=embed_dm)

    guild = interaction.guild
    if not guild:
        await interaction.response.send_message("Este comando solo funciona en un servidor.", ephemeral=True)
        return

    channel = guild.get_channel(Config.CH_CITACIONES)
    if not channel:
        await interaction.response.send_message("No se encontró el canal de citaciones.", ephemeral=True)
        return

    embed_channel = Embed(
        title="📢 Citación a sala de espera",
        description=f"**{usuario.display_name}** ha sido citado por el staff.\n\n"
                    "🔹 **Usuario citado:**\n"
                    f"{usuario.mention}\n\n"
                    "🔹 **Motivo:**\n"
                    f"{motivo}\n\n"
                    "🔹 **Qué debo hacer:**\n"
                    "Entra al canal de voz **📞 Esperando Soporte** y espera a ser atendido por el staff.\n\n"
                    f"🔹 **Solicitud por:** {interaction.user.mention}\n\n"
                    f"📞 **LLAMADOS REALIZADOS:** {intento}/3",
        color=Color.blue()
    )
    embed_channel.set_footer(text=get_footer())
    await channel.send(f"{usuario.mention}", embed=embed_channel)
    await interaction.response.send_message(f"Citación enviada a {usuario.mention}.", ephemeral=True)

@bot.tree.command(name="valorar-staff", description="Valora a un miembro del staff por su atención (1-10)")
@app_commands.describe(
    staff="Miembro del staff a valorar",
    puntuacion="Puntuación del 1 al 10",
    comentario="Comentario sobre la atención (opcional)"
)
async def valorar_staff(interaction: Interaction, staff: discord.Member, puntuacion: app_commands.Range[int, 1, 10], comentario: str = ""):
    if interaction.user.id == staff.id:
        await interaction.response.send_message("❌ No puedes valorarte a ti mismo.", ephemeral=True)
        return

    if not is_staff(staff):
        await interaction.response.send_message("❌ El usuario seleccionado no es miembro del staff.", ephemeral=True)
        return

    await db.add_valoracion(
        user_id=interaction.user.id,
        user_name=interaction.user.display_name,
        staff_id=staff.id,
        staff_name=staff.display_name,
        puntuacion=puntuacion,
        comentario=comentario,
        ticket_id=None
    )

    embed_resp = Embed(
        title="✅ ¡Gracias por valorar!",
        description=f"Has valorado a **{staff.display_name}** con **{puntuacion}/10**.\n"
                    f"Comentario: {comentario if comentario else 'Sin comentario'}",
        color=Color.green()
    )
    embed_resp.set_footer(text=get_footer())
    await interaction.response.send_message(embed=embed_resp, ephemeral=True)

    embed_log = Embed(
        title="⭐ Nueva valoración de staff (manual)",
        description=f"**Usuario:** {interaction.user.mention} ({interaction.user.id})\n"
                    f"**Staff valorado:** {staff.mention} ({staff.id})\n"
                    f"**Puntuación:** {puntuacion}/10\n"
                    f"**Comentario:** {comentario if comentario else 'Sin comentario'}",
        color=Color.gold(),
        timestamp=datetime.datetime.utcnow()
    )
    embed_log.set_footer(text=get_footer())
    await Logger.send_log(interaction.guild, Config.LOG_VALORACIONES, embed_log)

    embed_dm = Embed(
        title="⭐ Has recibido una valoración",
        description=f"**Usuario:** {interaction.user.mention} ({interaction.user.id})\n"
                    f"**Puntuación:** {puntuacion}/10\n"
                    f"**Comentario:** {comentario if comentario else 'Sin comentario'}",
        color=Color.gold()
    )
    embed_dm.set_footer(text=get_footer())
    await send_dm(staff, embed=embed_dm)

@bot.tree.command(name="sync", description="Sincroniza los comandos del bot (solo Propietario y Dueño)")
async def sync(interaction: Interaction):
    if not is_owner_or_dueno(interaction.user):
        await interaction.response.send_message("No tienes permisos para usar este comando.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        synced = await bot.tree.sync()
        await interaction.followup.send(f"✅ Comandos sincronizados: {len(synced)}", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Error al sincronizar: {e}", ephemeral=True)

# ===================== EJECUCIÓN =====================
if __name__ == "__main__":
    bot.run(Config.TOKEN)